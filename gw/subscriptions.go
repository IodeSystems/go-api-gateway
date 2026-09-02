package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IodeSystems/graphql-go/v2"
	"nhooyr.io/websocket"
)

// subscriptionBroker returns the gateway's shared subBroker, lazy-
// initialized on first use. nil when no cluster is configured.
func (g *Gateway) subscriptionBroker() *subBroker {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.broker == nil && g.cfg.cluster != nil {
		g.broker = newSubBroker(g.cfg.cluster.Conn)
	}
	return g.broker
}

// lookupPool returns the proto pool for (ns, ver) under g.mu.
func (g *Gateway) lookupPool(ns, ver string) (*pool, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	p := g.protoSlot(poolKey{namespace: ns, version: ver})
	return p, p != nil
}

// ---------------------------------------------------------------------
// graphql-transport-ws protocol
// https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
// ---------------------------------------------------------------------

const wsSubprotocol = "graphql-transport-ws"

const (
	msgConnInit    = "connection_init"
	msgConnAck     = "connection_ack"
	msgPing        = "ping"
	msgPong        = "pong"
	msgSubscribe   = "subscribe"
	msgNext        = "next"
	msgError       = "error"
	msgComplete    = "complete"
)

type wsMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type subscribePayload struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables,omitempty"`
	OperationName string         `json:"operationName,omitempty"`
}

// serveWebSocket handles a single graphql-transport-ws connection.
// Each connection multiplexes any number of subscriptions, each tied
// to its own context so completion / cancellation is per-id.
func (g *Gateway) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if g.draining.Load() {
		http.Error(w, "gateway is draining", http.StatusServiceUnavailable)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{wsSubprotocol},
	})
	if err != nil {
		return
	}
	if conn.Subprotocol() != wsSubprotocol {
		_ = conn.Close(websocket.StatusPolicyViolation, "expected "+wsSubprotocol+" subprotocol")
		return
	}
	defer conn.Close(websocket.StatusInternalError, "internal")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// Attach the upgrade request so the caller-id seam (and any
	// downstream HTTPRequestFromContext consumer) can read inbound
	// headers from the handshake the same way they do on the GraphQL
	// HTTP path.
	ctx = withHTTPRequest(ctx, r)

	// Register so Drain can fire all cancels.
	connKey := uintptr(0)
	g.wsMu.Lock()
	for k := uintptr(1); ; k++ {
		if _, exists := g.wsConns[k]; !exists {
			connKey = k
			g.wsConns[k] = cancel
			break
		}
	}
	g.wsMu.Unlock()
	defer func() {
		g.wsMu.Lock()
		delete(g.wsConns, connKey)
		g.wsMu.Unlock()
	}()

	// Per-connection write serializer — websocket.Conn isn't
	// safe for concurrent writes.
	var writeMu sync.Mutex
	writeMsg := func(m wsMessage) error {
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
		defer wcancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}

	// Per-id subscription cancels.
	var subsMu sync.Mutex
	subs := map[string]context.CancelFunc{}
	defer func() {
		subsMu.Lock()
		for _, c := range subs {
			c()
		}
		subsMu.Unlock()
	}()

	// connection_init must arrive within a short window. graphql-ws
	// reference impl uses 3s default; we follow.
	initCtx, initCancel := context.WithTimeout(ctx, 3*time.Second)
	defer initCancel()
	mt, raw, err := conn.Read(initCtx)
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "init timeout")
		return
	}
	if mt != websocket.MessageText {
		_ = conn.Close(websocket.StatusInvalidFramePayloadData, "expected text frame")
		return
	}
	var init wsMessage
	if err := json.Unmarshal(raw, &init); err != nil || init.Type != msgConnInit {
		_ = conn.Close(websocket.StatusPolicyViolation, "expected connection_init")
		return
	}
	if err := writeMsg(wsMessage{Type: msgConnAck}); err != nil {
		return
	}

	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var m wsMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		switch m.Type {
		case msgPing:
			_ = writeMsg(wsMessage{Type: msgPong})
		case msgPong:
			// no-op
		case msgSubscribe:
			if m.ID == "" {
				continue
			}
			var p subscribePayload
			if err := json.Unmarshal(m.Payload, &p); err != nil {
				_ = writeMsg(wsErrorMsg(m.ID, "bad payload: "+err.Error()))
				continue
			}
			subCtx, subCancel := context.WithCancel(ctx)
			subsMu.Lock()
			if existing, ok := subs[m.ID]; ok {
				existing()
			}
			subs[m.ID] = subCancel
			subsMu.Unlock()
			go g.runSubscription(subCtx, m.ID, p, writeMsg, func() {
				subsMu.Lock()
				delete(subs, m.ID)
				subsMu.Unlock()
			})
		case msgComplete:
			subsMu.Lock()
			if c, ok := subs[m.ID]; ok {
				c()
				delete(subs, m.ID)
			}
			subsMu.Unlock()
		}
	}
}

// runSubscription executes one graphql.Subscribe and pumps results
// back to the WebSocket as `next` frames. Sends `complete` when the
// upstream channel closes; sends `error` on initial subscription
// failure.
func (g *Gateway) runSubscription(
	ctx context.Context,
	id string,
	p subscribePayload,
	writeMsg func(wsMessage) error,
	cleanup func(),
) {
	defer cleanup()
	schema := g.schema.Load()
	if schema == nil {
		_ = writeMsg(wsErrorMsg(id, "schema not assembled"))
		return
	}
	results := graphql.Subscribe(graphql.Params{
		Schema:         *schema,
		RequestString:  p.Query,
		VariableValues: p.Variables,
		OperationName:  p.OperationName,
		Context:        ctx,
	})
	for res := range results {
		if ctx.Err() != nil {
			return
		}
		// graphql-transport-ws spec:
		//   error payload = Array<GraphQLError>
		//   next  payload = ExecutionResult ({data, errors, extensions})
		// Subscribe-time failures (no data) get an `error` frame; the
		// subscription is then over.
		if len(res.Errors) > 0 && res.Data == nil {
			b, err := json.Marshal(res.Errors)
			if err != nil {
				continue
			}
			_ = writeMsg(wsMessage{ID: id, Type: msgError, Payload: b})
			return
		}
		b, err := json.Marshal(res)
		if err != nil {
			continue
		}
		_ = writeMsg(wsMessage{ID: id, Type: msgNext, Payload: b})
	}
	_ = writeMsg(wsMessage{ID: id, Type: msgComplete})
}

func wsErrorMsg(id, message string) wsMessage {
	payload, _ := json.Marshal([]map[string]any{{"message": message}})
	return wsMessage{ID: id, Type: msgError, Payload: payload}
}

// isWebSocketUpgrade detects a graphql-transport-ws upgrade request
// so Handler can route it to serveWebSocket instead of HTTP.
func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, sp := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(sp, ",") {
			if strings.TrimSpace(p) == wsSubprotocol {
				return true
			}
		}
	}
	return false
}

