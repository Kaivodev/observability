package grpcobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aipay-dev/observability/logging"
	"github.com/aipay-dev/observability/requestmeta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type safeBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *safeBuffer) contents() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

type healthService struct {
	grpc_health_v1.UnimplementedHealthServer
	check func(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error)
}

func (s healthService) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return s.check(ctx, req)
}

func startRPC(t *testing.T, interceptor grpc.UnaryServerInterceptor, handler healthService, propagate bool) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	grpc_health_v1.RegisterHealthServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })}
	if propagate {
		opts = append(opts, grpc.WithUnaryInterceptor(UnaryClient()))
	}
	conn, err := grpc.NewClient("passthrough:///memory", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestUnaryPropagationAcrossTwoServices(t *testing.T) {
	var access, runtime safeBuffer
	appLog := logging.New(&runtime, slog.LevelInfo)
	makeServer := func(service string) *Server {
		s, err := NewServer(logging.New(&access, slog.LevelInfo).With("service", service), appLog.With("service", service), func(context.Context) (string, error) { return "verified-test-peer", nil })
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ids := requestmeta.New()
	ledger := startRPC(t, makeServer("ledger").Unary(), healthService{check: func(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
		if requestmeta.From(ctx) != ids {
			t.Error("ledger lost correlation")
		}
		appLog.InfoContext(ctx, "ledger diagnostic")
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
	}}, true)
	iam := startRPC(t, makeServer("iam").Unary(), healthService{check: func(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
		if requestmeta.From(ctx) != ids {
			t.Error("iam lost correlation")
		}
		appLog.InfoContext(ctx, "iam diagnostic")
		return grpc_health_v1.NewHealthClient(ledger).Check(ctx, req)
	}}, true)
	ctx, cancel := context.WithTimeout(requestmeta.With(context.Background(), ids), 5*time.Second)
	defer cancel()
	var header metadata.MD
	_, err := grpc_health_v1.NewHealthClient(iam).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: "private-body"}, grpc.Header(&header))
	if err != nil {
		t.Fatal(err)
	}
	if header.Get(RequestIDHeader)[0] != ids.RequestID || header.Get(TraceIDHeader)[0] != ids.TraceID {
		t.Fatal("incorrect response headers")
	}
	for _, output := range []string{access.contents(), runtime.contents()} {
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) != 2 {
			t.Fatalf("want two records: %s", output)
		}
		for _, line := range lines {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatal(err)
			}
			if record["request_id"] != ids.RequestID || record["trace_id"] != ids.TraceID {
				t.Fatal("log correlation mismatch")
			}
		}
		if strings.Contains(output, "private-body") {
			t.Fatal("RPC message leaked")
		}
	}
}

func TestUnaryFailureBoundaries(t *testing.T) {
	ids := requestmeta.New()
	for _, tc := range []struct {
		name          string
		md            metadata.MD
		authError     bool
		panicHandler  bool
		cancelHandler bool
		want          codes.Code
	}{
		{"auth failure", metadata.Pairs(RequestIDHeader, ids.RequestID, TraceIDHeader, ids.TraceID), true, false, false, codes.Unauthenticated},
		{"duplicate ID", metadata.Pairs(RequestIDHeader, ids.RequestID, RequestIDHeader, ids.RequestID, TraceIDHeader, ids.TraceID), false, false, false, codes.InvalidArgument},
		{"missing trace", metadata.Pairs(RequestIDHeader, ids.RequestID), false, false, false, codes.InvalidArgument},
		{"invalid trace", metadata.Pairs(RequestIDHeader, ids.RequestID, TraceIDHeader, "private-invalid-id"), false, false, false, codes.InvalidArgument},
		{"panic", nil, false, true, false, codes.Internal},
		{"canceled", nil, false, false, true, codes.Canceled},
		{"no correlation", nil, false, false, false, codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var access, runtime safeBuffer
			s, err := NewServer(logging.New(&access, slog.LevelInfo), logging.New(&runtime, slog.LevelInfo), func(context.Context) (string, error) {
				if tc.authError {
					return "private-unverified-name", errors.New("private-auth-error")
				}
				return "verified-peer", nil
			})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			interceptor := s.Unary()
			_, callErr := interceptor(metadata.NewIncomingContext(context.Background(), tc.md), "private-body", &grpc.UnaryServerInfo{FullMethod: "/test/Check"}, func(ctx context.Context, _ any) (any, error) {
				called = true
				if !requestmeta.From(ctx).Valid() {
					t.Error("missing generated IDs")
				}
				if tc.panicHandler {
					panic("private-panic")
				}
				if tc.cancelHandler {
					return nil, context.Canceled
				}
				return nil, nil
			})
			if resultCode(callErr) != tc.want {
				t.Fatalf("unexpected error %v", callErr)
			}
			if (tc.want == codes.Unauthenticated || tc.want == codes.InvalidArgument) && called {
				t.Fatal("rejected request reached business handler")
			}
			var record map[string]any
			if err := json.Unmarshal([]byte(access.contents()), &record); err != nil {
				t.Fatal(err)
			}
			if record["code"] != tc.want.String() || record["failed"] != (tc.want != codes.OK) {
				t.Fatalf("incorrect outcome %v", record)
			}
			if tc.authError && record["trace_id"] == ids.TraceID {
				t.Fatal("unverified peer supplied correlation")
			}
			if strings.Contains(access.contents()+runtime.contents(), "private-") {
				t.Fatal("private content leaked")
			}
			if tc.panicHandler && !strings.Contains(runtime.contents(), "stack") {
				t.Fatal("panic lacks stack")
			}
		})
	}
	if _, err := NewServer(slog.Default(), slog.Default(), nil); err == nil {
		t.Fatal("missing verifier accepted")
	}
}

func TestUnaryClientOverwritesIDsWithoutMutatingMetadata(t *testing.T) {
	ids := requestmeta.New()
	md := metadata.Pairs(RequestIDHeader, "untrusted", TraceIDHeader, "untrusted", "authorization", "private-token")
	ctx := metadata.NewOutgoingContext(requestmeta.With(context.Background(), ids), md)
	err := UnaryClient()(ctx, "/test/Check", nil, nil, nil, func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		if len(outgoing.Get(RequestIDHeader)) != 1 || outgoing.Get(RequestIDHeader)[0] != ids.RequestID || outgoing.Get(TraceIDHeader)[0] != ids.TraceID {
			t.Fatal("IDs not replaced")
		}
		if outgoing.Get("authorization")[0] != "private-token" {
			t.Fatal("unrelated metadata lost")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if md.Get(RequestIDHeader)[0] != "untrusted" {
		t.Fatal("caller's metadata mutated")
	}
}
