package gateway

import (
	"context"
	"net/http"
	"strings"
	"time"

	aav1 "github.com/iodesystems/gwag/v2/gw/proto/adminauth/v1"
	"google.golang.org/grpc"
)

// adminAuthorizerNamespace is the reserved registration namespace for
// the AdminAuthorizer delegate. A service implementing the Authorize
// RPC registers under "_admin_auth/v1" and the gateway routes
// AdminMiddleware consultations to it.
const adminAuthorizerNamespace = "_admin_auth"

// adminDelegateOutcome is what consultAdminDelegate returns to
// AdminMiddleware. The middleware uses this to decide whether to
// short-circuit (accept/reject) or fall through to boot-token check.
type adminDelegateOutcome int

const (
	// No usable delegate result; the middleware must fall through to
	// boot-token bearer verification. Covers: no delegate registered,
	// delegate replied UNSPECIFIED / UNAVAILABLE / NOT_CONFIGURED, or
	// transport itself failed.
	adminDelegateFallthrough adminDelegateOutcome = iota
	// Delegate authorized the request. Middleware accepts without
	// checking the boot token.
	adminDelegateAccept
	// Delegate explicitly rejected. Middleware returns 401 without
	// falling through.
	adminDelegateReject
)

// consultAdminDelegate calls Authorize on the registered
// AdminAuthorizer if one is present. Returns the decision the
// middleware should act on, plus an optional reason string for logs
// (never surfaced to clients).
func (g *Gateway) consultAdminDelegate(ctx context.Context, r *http.Request) (adminDelegateOutcome, string) {
	pool, ok := g.lookupPool(adminAuthorizerNamespace, "v1")
	if !ok {
		return adminDelegateFallthrough, ""
	}
	rep := pool.pickReplica()
	if rep == nil {
		return adminDelegateFallthrough, "no live _admin_auth/v1 replicas"
	}
	conn, ok := rep.conn.(grpc.ClientConnInterface)
	if !ok {
		return adminDelegateFallthrough, "delegate replica conn not usable"
	}
	client := aav1.NewAdminAuthorizerClient(conn)
	dctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := client.Authorize(dctx, &aav1.AuthorizeRequest{
		Token:      bearerFromRequest(r),
		Method:     r.Method,
		Path:       r.URL.Path,
		RemoteAddr: r.RemoteAddr,
	})
	if err != nil {
		return adminDelegateFallthrough, err.Error()
	}
	switch resp.GetCode() {
	case aav1.AdminAuthCode_ADMIN_AUTH_CODE_OK:
		return adminDelegateAccept, resp.GetReason()
	case aav1.AdminAuthCode_ADMIN_AUTH_CODE_DENIED:
		return adminDelegateReject, resp.GetReason()
	default:
		return adminDelegateFallthrough, resp.GetReason()
	}
}

// bearerFromRequest extracts the raw token string after the `Bearer `
// prefix in Authorization. Returns "" if no Authorization header was
// sent or it didn't use the Bearer scheme.
func bearerFromRequest(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return ""
	}
	return strings.TrimSpace(authz[len(prefix):])
}
