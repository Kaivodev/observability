package metrics

import (
	"bytes"
	"context"
	"log/slog"
	"maps"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	collector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type receiver struct {
	collector.UnimplementedMetricsServiceServer
	handle func(context.Context, *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error)
}

func (s receiver) Export(ctx context.Context, req *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
	return s.handle(ctx, req)
}

func startReceiver(t *testing.T, s receiver) (string, *grpc.Server) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	collector.RegisterMetricsServiceServer(server, s)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	return "http://" + l.Addr().String(), server
}

func options(t *testing.T) Options {
	t.Helper()
	cfg, err := loadValues(nil)
	if err != nil {
		t.Fatal(err)
	}
	return Options{Config: cfg, Service: "ledger", Version: "test-version", Instance: "test-pod"}
}

func openRuntime(t *testing.T, o Options, log *slog.Logger) *Runtime {
	t.Helper()
	rt, err := Open(context.Background(), o, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	return rt
}

func counter(t *testing.T, rt *Runtime) metric.Int64Counter {
	t.Helper()
	c, err := rt.Meter("test").Int64Counter("kaivo.test.counter")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDisabledExport(t *testing.T) {
	global := otel.GetMeterProvider()
	rt := openRuntime(t, options(t), slog.New(slog.DiscardHandler))
	counter(t, rt).Add(context.Background(), 1)
	if rt.Provider() == nil || rt.exporter != nil || rt.ExportFailures() != 0 || otel.GetMeterProvider() != global {
		t.Fatal("unexpected disabled-export runtime")
	}
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicOTLPResourcesAttributesAndOverflow(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "delta")
	t.Setenv("OTEL_METRICS_EXEMPLAR_FILTER", "always_on")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	requests := make(chan *collector.ExportMetricsServiceRequest, 4)
	o := options(t)
	o.Config.Endpoint, _ = startReceiver(t, receiver{handle: func(ctx context.Context, r *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
		select {
		case requests <- r:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &collector.ExportMetricsServiceResponse{}, nil
	}})
	o.Config.Interval, o.Config.ExportTimeout = time.Second, 800*time.Millisecond
	o.Config.Environment, o.Config.CardinalityLimit = "staging", 100
	rt := openRuntime(t, o, slog.New(slog.DiscardHandler))
	c := counter(t, rt)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled,
	}))
	for i := range 125 {
		c.Add(ctx, 1, metric.WithAttributes(attribute.Int("kind", i), attribute.String("User-Id", "private-canary"),
			attribute.String("peer.address", "private-canary")))
	}
	var r *collector.ExportMetricsServiceRequest
	select {
	case r = <-requests:
	case <-time.After(4 * time.Second):
		t.Fatal("no periodic OTLP export")
	}
	if len(r.ResourceMetrics) != 1 || strings.Contains(r.String(), "private-canary") {
		t.Fatal("missing resource or leaked filtered value")
	}
	rm := r.ResourceMetrics[0]
	res := map[string]string{}
	for _, kv := range rm.Resource.Attributes {
		res[kv.Key] = kv.Value.GetStringValue()
	}
	if !maps.Equal(res, map[string]string{"service.name": "ledger", "service.version": "test-version", "service.instance.id": "test-pod", "deployment.environment.name": "staging"}) {
		t.Fatalf("unexpected resource: %v", res)
	}
	if len(rm.ScopeMetrics) != 1 || rm.ScopeMetrics[0].Scope.Name != "test" || len(rm.ScopeMetrics[0].Metrics) != 1 {
		t.Fatal("missing counter scope or metric")
	}
	m := rm.ScopeMetrics[0].Metrics[0]
	sum := m.GetSum()
	if m.Name != "kaivo.test.counter" || sum.GetAggregationTemporality() != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE ||
		len(sum.GetDataPoints()) != 100 {
		t.Fatal("unexpected temporality or cardinality")
	}
	var total, overflow int64
	for _, p := range sum.DataPoints {
		if len(p.Attributes) != 1 || len(p.Exemplars) != 0 {
			t.Fatal("unexpected datapoint attributes or exemplars")
		}
		a := p.Attributes[0]
		switch a.Key {
		case "kind":
		case "otel.metric.overflow":
			if !a.Value.GetBoolValue() {
				t.Fatal("invalid overflow attribute")
			}
			overflow += p.GetAsInt()
		default:
			t.Fatalf("unexpected key %q", a.Key)
		}
		total += p.GetAsInt()
	}
	if total != 125 || overflow != 26 {
		t.Fatalf("total %d, overflow %d", total, overflow)
	}
}

func TestOTLPFailureEpisodesAndRecovery(t *testing.T) {
	var calls atomic.Uint64
	o := options(t)
	o.Config.Endpoint, _ = startReceiver(t, receiver{handle: func(context.Context, *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
		switch calls.Add(1) {
		case 1, 2, 5:
			return nil, status.Error(codes.Unavailable, "private-collector-diagnostic")
		case 3:
			return &collector.ExportMetricsServiceResponse{PartialSuccess: &collector.ExportMetricsPartialSuccess{
				RejectedDataPoints: 1, ErrorMessage: "private-collector-diagnostic",
			}}, nil
		default:
			return &collector.ExportMetricsServiceResponse{}, nil
		}
	}})
	var logs bytes.Buffer
	rt := openRuntime(t, o, slog.New(slog.NewTextHandler(&logs, nil)))
	counter(t, rt).Add(context.Background(), 1)
	for range 6 {
		if err := rt.provider.ForceFlush(context.Background()); err != nil {
			t.Fatal("unhandled exporter error")
		}
	}
	if err := rt.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rt.ExportFailures() != 4 || calls.Load() != 7 {
		t.Fatalf("failures %d, exports %d", rt.ExportFailures(), calls.Load())
	}
	if strings.Count(logs.String(), "metrics batch export failed") != 2 || strings.Count(logs.String(), "metrics export recovered") != 2 ||
		strings.Contains(logs.String(), "private-collector-diagnostic") {
		t.Fatal("unexpected export diagnostics")
	}
	if err := rt.Shutdown(context.Background()); err != nil || calls.Load() != 7 {
		t.Fatal("shutdown flushed twice")
	}
}

func TestCollectorOutageAndShutdownBudgets(t *testing.T) {
	for _, test := range []struct {
		name                  string
		stopServer            bool
		timeout, parentBudget time.Duration
	}{
		{"unreachable", true, 60 * time.Millisecond, time.Second},
		{"stalled", false, 60 * time.Millisecond, time.Second},
		{"caller-deadline", false, time.Second, 60 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{}, 2)
			endpoint, server := startReceiver(t, receiver{handle: func(ctx context.Context, _ *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}})
			if test.stopServer {
				server.Stop()
			}
			o := options(t)
			o.Config.Endpoint, o.Config.ShutdownTimeout = endpoint, test.timeout
			o.Config.Interval, o.Config.ExportTimeout = time.Second, 900*time.Millisecond
			start := time.Now()
			rt := openRuntime(t, o, slog.New(slog.DiscardHandler))
			if time.Since(start) > 500*time.Millisecond {
				t.Fatal("startup waited on collector")
			}
			c := counter(t, rt)
			c.Add(context.Background(), 1)
			if !test.stopServer {
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("collector never received periodic export")
				}
			}
			start = time.Now()
			for range 1000 {
				c.Add(context.Background(), 1)
			}
			if time.Since(start) > 250*time.Millisecond {
				t.Fatal("recording waited for export")
			}
			ctx, cancel := context.WithTimeout(context.Background(), test.parentBudget)
			defer cancel()
			start = time.Now()
			err := rt.Shutdown(ctx)
			if time.Since(start) > min(test.timeout, test.parentBudget)+150*time.Millisecond {
				t.Fatal("shutdown exceeded budget")
			}
			if !test.stopServer && err != errShutdown {
				t.Fatalf("expected safe shutdown error, got %v", err)
			}
			select {
			case <-rt.done:
			case <-time.After(time.Second):
				t.Fatal("SDK failed to release exporter")
			}
			if rt.ExportFailures() == 0 {
				t.Fatal("outage not counted")
			}
		})
	}
}

func TestConcurrentShutdownUsesCallerBudgetAndFlushesOnce(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Uint64
	o := options(t)
	o.Config.Endpoint, _ = startReceiver(t, receiver{handle: func(ctx context.Context, _ *collector.ExportMetricsServiceRequest) (*collector.ExportMetricsServiceResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return &collector.ExportMetricsServiceResponse{}, nil
		}
	}})
	var logs bytes.Buffer
	rt := openRuntime(t, o, slog.New(slog.NewTextHandler(&logs, nil)))
	counter(t, rt).Add(context.Background(), 1)
	finished := make(chan error, 1)
	go func() { finished <- rt.Shutdown(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("final export did not begin")
	}
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := rt.Shutdown(short)
	close(release)
	if err != errShutdown || time.Since(start) > 250*time.Millisecond {
		t.Fatalf("concurrent deadline ignored: %v", err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := rt.Shutdown(short); err != nil || calls.Load() != 1 {
		t.Fatal("first shutdown result not preserved")
	}
	if strings.Count(logs.String(), "metrics shutdown incomplete") != 1 {
		t.Fatal("missing safe shutdown warning")
	}
}
