// Sign-as-API worked example. Demonstrates the post-§2.3 model:
// the gateway is a pure signer (no pull-delegate), a downstream
// service holds the signer secret, does its own authz, and calls
// the gateway over gRPC to mint subscription tokens.
//
// Three things run in-process so this stays one binary:
//
//   - gateway     — embedded NATS, control-plane gRPC at :50090,
//                   GraphQL at :8080. Configured with a signer
//                   secret; the bearer gate fires for any wire
//                   call to SignSubscriptionToken.
//   - control-plane gRPC server on :50090 — the calling service
//                   uses this exact wire path, presenting the
//                   signer secret as bearer.
//   - auth-shim   — HTTP service on :8090. Receives client
//                   subscribe-token requests, does business authz
//                   (see allowSubscribe below), then signs via
//                   the gateway. Returns hmac+ts to the client.
//
// Run:
//
//	go run ./examples/sign
//
// Then:
//
//	$ curl -sS -X POST http://localhost:8090/subscribe-token \
//	    -H 'Authorization: Bearer demo-user-alice' \
//	    -H 'Content-Type: application/json' \
//	    -d '{"channel": "events.user.alice", "ttl_seconds": 60}'
//	{"hmac":"…","timestamp":1778012345,"channel":"events.user.alice","kid":""}
//
//	$ curl -sS -X POST http://localhost:8090/subscribe-token \
//	    -H 'Authorization: Bearer demo-user-alice' \
//	    -d '{"channel": "events.user.bob"}'   # alice can't sign bob's
//	{"error":"forbidden"}
//
// The gateway never sees the user identity — that's the auth-shim's
// job. The gateway just trusts the signer-secret bearer ("this
// service speaks for me"); the service has the request context to
// make the per-user decision.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	gateway "github.com/iodesystems/gwag/v2/gw"
	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

const (
	defaultGatewayAddr = "127.0.0.1:8080"
	defaultControlAddr = "127.0.0.1:50090"
	defaultShimAddr    = "127.0.0.1:8090"

	// Demo secrets — never reuse in production. The signer-secret
	// gates the gRPC sign endpoint; the subscribe-secret is the HMAC
	// shared with anyone verifying tokens (the gateway itself, here).
	signerSecretHex    = "11111111111111111111111111111111"
	subscribeSecretHex = "22222222222222222222222222222222"
)

func main() {
	gatewayAddrFlag := flag.String("graphql", defaultGatewayAddr, "GraphQL HTTP listen address")
	controlAddrFlag := flag.String("control-plane", defaultControlAddr, "Control-plane gRPC listen address")
	shimAddrFlag := flag.String("shim", defaultShimAddr, "Auth-shim HTTP listen address")
	flag.Parse()
	gatewayAddr := *gatewayAddrFlag
	controlAddr := *controlAddrFlag
	shimAddr := *shimAddrFlag

	signerSecret, _ := hex.DecodeString(signerSecretHex)
	subscribeSecret, _ := hex.DecodeString(subscribeSecretHex)

	gw := gateway.New(
		gateway.WithSubscriptionAuth(gateway.SubscriptionAuthOptions{
			Secret: subscribeSecret,
		}),
		gateway.WithSignerSecret(signerSecret),
	)
	defer gw.Close()

	// Boot the control-plane gRPC server. The auth-shim talks to
	// this addr, so the call really crosses the wire — peer.FromContext
	// is non-nil and the bearer gate fires (the whole point of the
	// example).
	cpLis, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatalf("listen control plane: %v", err)
	}
	cpSrv := grpc.NewServer()
	cpv1.RegisterControlPlaneServer(cpSrv, gw.ControlPlane())
	go func() {
		log.Printf("control plane listening on %s", controlAddr)
		if err := cpSrv.Serve(cpLis); err != nil {
			log.Printf("control plane serve: %v", err)
		}
	}()

	// Public GraphQL surface (queries, mutations, subscriptions). Not
	// load-bearing for the sign demo, but mounted so a real client
	// could verify the minted token by opening a graphql-ws
	// subscription against it.
	go func() {
		log.Printf("graphql listening on %s", gatewayAddr)
		if err := http.ListenAndServe(gatewayAddr, gw.Handler()); err != nil {
			log.Printf("graphql serve: %v", err)
		}
	}()

	// Auth-shim: dial the gateway's control plane (signing client),
	// expose a small HTTP API for clients.
	cpConn, err := grpc.NewClient(controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial control plane: %v", err)
	}
	defer cpConn.Close()
	signClient := cpv1.NewControlPlaneClient(cpConn)

	shim := &authShim{sign: signClient, signerHex: signerSecretHex}
	mux := http.NewServeMux()
	mux.HandleFunc("/subscribe-token", shim.handleSubscribeToken)
	go func() {
		log.Printf("auth-shim listening on %s", shimAddr)
		if err := http.ListenAndServe(shimAddr, mux); err != nil {
			log.Printf("auth-shim serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down")
	cpSrv.GracefulStop()
}

type authShim struct {
	sign      cpv1.ControlPlaneClient
	signerHex string
}

type subscribeTokenRequest struct {
	Channel    string `json:"channel"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type subscribeTokenResponse struct {
	HMAC      string `json:"hmac"`
	Timestamp int64  `json:"timestamp"`
	Channel   string `json:"channel"`
	Kid       string `json:"kid"`
}

func (s *authShim) handleSubscribeToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	user := userFromBearer(r.Header.Get("Authorization"))
	if user == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthenticated"})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
		return
	}
	var req subscribeTokenRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json"})
		return
	}
	if req.Channel == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "channel required"})
		return
	}
	// Business authz: the service has full request context — the
	// caller's identity, IP, headers, plus its own datastore. The
	// gateway can't make this call; only the service can.
	if !allowSubscribe(user, req.Channel) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = 60
	}

	// Sign via the gateway's gRPC. The signer-secret authenticates
	// the *service* to the gateway — "this caller speaks for me, sign
	// what they ask". The gateway has zero opinion about the user.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.signerHex)
	resp, err := s.sign.SignSubscriptionToken(ctx, &cpv1.SignSubscriptionTokenRequest{
		Channel:    req.Channel,
		TtlSeconds: ttl,
	})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "sign rpc: " + err.Error()})
		return
	}
	if resp.GetCode() != cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error":  "sign rejected",
			"code":   resp.GetCode().String(),
			"reason": resp.GetReason(),
		})
		return
	}

	writeJSON(w, http.StatusOK, subscribeTokenResponse{
		HMAC:      resp.GetHmac(),
		Timestamp: resp.GetTimestampUnix(),
		Channel:   req.Channel,
		Kid:       resp.GetKid(),
	})
}

// allowSubscribe is the toy authz rule for the demo — alice can sign
// anything starting with "events.user.alice" and nothing else. Real
// services consult their entitlement model here.
func allowSubscribe(user, channel string) bool {
	prefix := "events.user." + user
	return channel == prefix || strings.HasPrefix(channel, prefix+".")
}

// userFromBearer extracts a user id from `Authorization: Bearer
// demo-user-<id>`. The toy scheme keeps the example self-contained;
// real services validate JWTs / sessions / etc. here.
func userFromBearer(authz string) string {
	const prefix = "Bearer demo-user-"
	if !strings.HasPrefix(authz, prefix) {
		return ""
	}
	return authz[len(prefix):]
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Fprintf(os.Stderr, "writeJSON: %v\n", err)
	}
}
