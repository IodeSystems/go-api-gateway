package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	cpv1 "github.com/iodesystems/gwag/v2/gw/proto/controlplane/v1"
)

const defaultSkewWindow = 5 * time.Minute

// subscribeAuthError is returned to graphql-go when verification fails;
// the WebSocket layer formats it into a graphql-ws `error` frame whose
// extensions carry the SubscribeAuthCode for operator/client triage.
type subscribeAuthError struct {
	Code   cpv1.SubscribeAuthCode
	Reason string // optional; surfaced to clients only when --debug-auth (TODO)
}

func (e *subscribeAuthError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("%s: %s", e.Code.String(), e.Reason)
	}
	return e.Code.String()
}

func (e *subscribeAuthError) Extensions() map[string]any {
	return map[string]any{
		"subscribeAuthCode": e.Code.String(),
	}
}

// verifySubscribe checks the (channel, hmac, timestamp, kid) tuple
// against the gateway's SubscriptionAuthOptions. Returns OK or the
// appropriate failure code. The metric counter is incremented by the
// caller.
func (g *Gateway) verifySubscribe(channel string, args map[string]any) cpv1.SubscribeAuthCode {
	if g.cfg.subAuth.Insecure {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK
	}
	if !g.cfg.subAuth.hasAnySecret() {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_NOT_CONFIGURED
	}

	hmacRaw, ok := args["hmac"]
	if !ok {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_MALFORMED
	}
	hmacStr, ok := hmacRaw.(string)
	if !ok || hmacStr == "" {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_MALFORMED
	}
	tsRaw, ok := args["timestamp"]
	if !ok {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_MALFORMED
	}
	ts, ok := asInt64(tsRaw)
	if !ok {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_MALFORMED
	}
	kid, kidOK := optionalString(args, "kid")
	if !kidOK {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_MALFORMED
	}

	secret, found := g.cfg.subAuth.lookupSecret(kid)
	if !found {
		// kid != "" → caller asked for a specific key the operator
		// hasn't loaded. kid == "" means no Secret / Secrets[""] is
		// configured but at least one keyed secret is — operator
		// hasn't authorized unkeyed tokens.
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_UNKNOWN_KID
	}

	skew := g.cfg.subAuth.SkewWindow
	if skew == 0 {
		skew = defaultSkewWindow
	}
	now := time.Now().Unix()
	if ts < now-int64(skew.Seconds()) {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_TOO_OLD
	}
	if ts > now+int64(skew.Seconds()) {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_TOO_NEW
	}

	expected := computeSubscribeHMAC(secret, kid, channel, ts)
	provided, err := base64.StdEncoding.DecodeString(hmacStr)
	if err != nil || !hmac.Equal(expected, provided) {
		return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_SIGNATURE_MISMATCH
	}
	return cpv1.SubscribeAuthCode_SUBSCRIBE_AUTH_CODE_OK
}

// hasAnySecret reports whether any HMAC secret is configured (legacy
// Secret or any Secrets entry).
func (o SubscriptionAuthOptions) hasAnySecret() bool {
	if len(o.Secret) > 0 {
		return true
	}
	for _, v := range o.Secrets {
		if len(v) > 0 {
			return true
		}
	}
	return false
}

// lookupSecret resolves a kid to its HMAC secret. The legacy `Secret`
// field wins over `Secrets[""]` to keep a deployment that sets only
// Secret behaving exactly as before. Returns (nil, false) when the kid
// has no entry — caller emits UNKNOWN_KID.
func (o SubscriptionAuthOptions) lookupSecret(kid string) ([]byte, bool) {
	if kid == "" && len(o.Secret) > 0 {
		return o.Secret, true
	}
	if v, ok := o.Secrets[kid]; ok && len(v) > 0 {
		return v, true
	}
	return nil, false
}

// computeSubscribeHMAC is the canonical signing function. Surface in
// the public API as SignSubscribeToken / SignSubscribeTokenWithKid so
// external services using the same secret can mint compatible tokens
// locally.
//
// kid binds the token to a specific key id so swapping kid can't
// replay a token across keys. Empty kid keeps the legacy single-key
// payload "<channel>\n<timestamp>"; non-empty kid uses the rotated
// payload "<kid>\n<channel>\n<timestamp>".
func computeSubscribeHMAC(secret []byte, kid, channel string, timestampUnix int64) []byte {
	h := hmac.New(sha256.New, secret)
	if kid == "" {
		fmt.Fprintf(h, "%s\n%d", channel, timestampUnix)
	} else {
		fmt.Fprintf(h, "%s\n%s\n%d", kid, channel, timestampUnix)
	}
	return h.Sum(nil)
}

// SignSubscribeToken returns the (base64-hmac, timestamp) pair for a
// channel using the given secret. Intended for callers that hold the
// shared secret and mint tokens locally — pair with the gateway's
// verifySubscribe on the read side. Equivalent to
// SignSubscribeTokenWithKid(secret, "", channel, ttlSeconds).
//
// ttlSeconds is currently informational; the gateway's SkewWindow
// bounds replay regardless. A future version may pin tokens to an
// explicit expiry rather than a wall-clock window.
//
// Stability: stable
func SignSubscribeToken(secret []byte, channel string, ttlSeconds int64) (hmacB64 string, timestampUnix int64) {
	hmacB64, _, timestampUnix = SignSubscribeTokenWithKid(secret, "", channel, ttlSeconds)
	return hmacB64, timestampUnix
}

// SignSubscribeTokenWithKid is the rotation-aware signer. Pair with
// `SubscriptionAuthOptions.Secrets[kid]` on the verifier side and pass
// `kid` alongside `hmac` / `timestamp` in the subscribe args. Empty
// kid produces a legacy token (verifies against `Secret`).
//
// Stability: stable
func SignSubscribeTokenWithKid(secret []byte, kid, channel string, ttlSeconds int64) (hmacB64, kidOut string, timestampUnix int64) {
	timestampUnix = time.Now().Unix()
	mac := computeSubscribeHMAC(secret, kid, channel, timestampUnix)
	return base64.StdEncoding.EncodeToString(mac), kid, timestampUnix
}

// optionalString reads args[name] as a string. Missing / null is OK
// (returns ""). Wrong type is malformed.
func optionalString(args map[string]any, name string) (string, bool) {
	raw, ok := args[name]
	if !ok || raw == nil {
		return "", true
	}
	s, ok := raw.(string)
	if !ok {
		return "", false
	}
	return s, true
}

// asInt64 coerces a numeric arg to int64 so we can compare timestamps.
// Sources: graphql-go's int (graphql.Int field), float64 (JSON default
// without UseNumber), json.Number (HTTP/JSON ingress decoder), and
// string (HTTP query params on SSE subscribe). Anything else is a
// type error the caller surfaces as MALFORMED.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case interface{ Int64() (int64, error) }:
		// json.Number satisfies this (and isn't directly importable
		// here without bringing encoding/json into a non-JSON file).
		x, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return x, true
	case string:
		x, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0, false
		}
		return x, true
	}
	return 0, false
}
