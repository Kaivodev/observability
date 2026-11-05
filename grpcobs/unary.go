// Package grpcobs provides unary RPC correlation, access logs and panic recovery.
// It never logs request/response messages, metadata dumps or raw error messages.
package grpcobs

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/Kaivodev/observability/requestmeta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	RequestIDHeader = "x-request-id"
	TraceIDHeader   = "x-trace-id"
)

// Authenticate must verify the transport/service identity and return its stable
// service name. It must not derive identity from an unverified metadata field.
// Business authorization and resource scope checks still belong to the service.
type Authenticate func(context.Context) (string, error)

type Server struct {
	access       *slog.Logger
	runtime      *slog.Logger
	authenticate Authenticate
}

var peerName = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

// NewServer requires a verifier so accidentally omitting authentication cannot
// silently make inbound correlation metadata trusted.
func NewServer(access, runtime *slog.Logger, authenticate Authenticate) (*Server, error) {
	if access == nil || runtime == nil || authenticate == nil {
		return nil, errors.New("RPC logging requires access/runtime loggers and a service identity verifier")
	}
	return &Server{access: access, runtime: runtime, authenticate: authenticate}, nil
}

func (s *Server) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		started := time.Now()
		// Start with local IDs so authentication failures never echo untrusted IDs.
		ctx = requestmeta.With(ctx, requestmeta.New())
		peerService := ""
		defer func() {
			if recover() != nil {
				s.runtime.ErrorContext(ctx, "rpc handler panic", "method", info.FullMethod, "stack", string(debug.Stack()))
				response, err = nil, status.Error(codes.Internal, "Internal Server Error")
			}
			code := resultCode(err)
			level := slog.LevelInfo
			if code != codes.OK {
				level = slog.LevelWarn
			}
			s.access.Log(ctx, level, "rpc request", "protocol", "grpc", "direction", "inbound", "method", info.FullMethod,
				"peer_service", peerService, "code", code.String(), "failed", code != codes.OK, "duration_ms", time.Since(started).Milliseconds())
		}()
		peer, authErr := s.authenticate(ctx)
		if authErr != nil || !peerName.MatchString(peer) {
			return nil, status.Error(codes.Unauthenticated, "Service authentication required")
		}
		peerService = peer
		inbound, present, parseErr := incoming(ctx)
		if parseErr != nil {
			return nil, parseErr
		}
		if present {
			ctx = requestmeta.With(ctx, inbound)
		}
		ids := requestmeta.From(ctx)
		_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, ids.RequestID, TraceIDHeader, ids.TraceID))
		return handler(ctx, req)
	}
}

// UnaryClient propagates canonical correlation metadata without changing the
// caller's metadata map. No client access log is emitted: the receiving service
// records its own inbound access event. Jobs should create context IDs at entry.
func UnaryClient() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
		ids := requestmeta.From(ctx)
		if !ids.Valid() {
			ids = requestmeta.New()
			ctx = requestmeta.With(ctx, ids)
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Set(RequestIDHeader, ids.RequestID)
		md.Set(TraceIDHeader, ids.TraceID)
		return invoker(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, options...)
	}
}

func incoming(ctx context.Context) (requestmeta.Info, bool, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	requests, traces := md.Get(RequestIDHeader), md.Get(TraceIDHeader)
	if len(requests) == 0 && len(traces) == 0 {
		return requestmeta.Info{}, false, nil
	}
	if len(requests) == 1 && len(traces) == 1 {
		ids := requestmeta.Info{RequestID: requests[0], TraceID: traces[0]}
		if ids.Valid() {
			return ids, true, nil
		}
	}
	return requestmeta.Info{}, false, status.Error(codes.InvalidArgument, "Invalid correlation metadata")
}

func resultCode(err error) codes.Code {
	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	return status.Code(err)
}
