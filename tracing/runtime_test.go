package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aipay-dev/observability/logging"
	"github.com/aipay-dev/observability/requestmeta"
	"go.opentelemetry.io/otel/trace"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type captureCollector struct {
	collector.UnimplementedTraceServiceServer
	mu       sync.Mutex
	requests []*collector.ExportTraceServiceRequest
	stall    bool
	entered  chan struct{}
}

func (c *captureCollector) Export(ctx context.Context, r *collector.ExportTraceServiceRequest) (*collector.ExportTraceServiceResponse, error) {
	if c.stall {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c.mu.Lock()
	c.requests = append(c.requests, r)
	c.mu.Unlock()
	return &collector.ExportTraceServiceResponse{}, nil
}
func collectorServer(t *testing.T, c *captureCollector) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	collector.RegisterTraceServiceServer(server, c)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return "http://" + listener.Addr().String()
}
func options() Options {
	return Options{Config: DefaultConfig(), Service: "iam", Version: "test", Instance: "test-instance"}
}

func TestRealOTLPExportAndCanonicalLogIDs(t *testing.T) {
	c := &captureCollector{}
	o := options()
	o.Config.Endpoint = collectorServer(t, c)
	runtime, err := Open(context.Background(), o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := runtime.Tracer("test").Start(context.Background(), "test.request", trace.WithSpanKind(trace.SpanKindServer))
	ctx = requestmeta.With(ctx, requestmeta.New())
	ids := requestmeta.From(ctx)
	var output bytes.Buffer
	log := logging.New(&output, slog.LevelInfo).With("trace_id", "stale-bound-trace", "span_id", "stale-bound-span")
	log.InfoContext(ctx, "request", "trace_id", "stale-record-trace", "span_id", "stale-record-span")
	var record map[string]any
	if err = json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["trace_id"] != span.SpanContext().TraceID().String() || record["span_id"] != span.SpanContext().SpanID().String() || ids.TraceID != record["trace_id"] {
		t.Fatal("span/log correlation drift")
	}
	if strings.Contains(output.String(), "stale-") {
		t.Fatal("caller attributes overrode canonical trace IDs")
	}
	span.End()
	if err = runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) != 1 || len(c.requests[0].ResourceSpans) != 1 {
		t.Fatalf("unexpected exported batches: %d", len(c.requests))
	}
	resource := c.requests[0].ResourceSpans[0]
	fields := map[string]string{}
	for _, a := range resource.Resource.Attributes {
		fields[a.Key] = a.Value.GetStringValue()
	}
	if fields["service.name"] != "iam" || fields["service.instance.id"] != "test-instance" || fields["service.version"] != "test" {
		t.Fatal("missing service resource metadata")
	}
	exported := resource.ScopeSpans[0].Spans[0]
	traceID := span.SpanContext().TraceID()
	if !bytes.Equal(exported.TraceId, traceID[:]) {
		t.Fatal("wrong exported trace")
	}
}

func TestExportStallDoesNotBlockRequestsAndShutdownIsBounded(t *testing.T) {
	c := &captureCollector{stall: true, entered: make(chan struct{}, 1)}
	o := options()
	o.Config.Endpoint = collectorServer(t, c)
	o.Config.QueueSize = 2
	o.Config.BatchSize = 1
	o.Config.ExportTimeout = 500 * time.Millisecond
	o.Config.ShutdownTimeout = 30 * time.Millisecond
	runtime, err := Open(context.Background(), o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	_, span := runtime.Tracer("test").Start(context.Background(), "first")
	span.End()
	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("export did not begin")
	}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_, s := runtime.Tracer("test").Start(context.Background(), "busy")
		s.End()
	}
	if time.Since(start) > time.Second {
		t.Fatal("full telemetry buffer blocked application")
	}
	start = time.Now()
	err = runtime.Shutdown(context.Background())
	if err == nil || time.Since(start) > time.Second {
		t.Fatal("shutdown did not enforce remaining budget")
	}
}

func TestCarrierSurvivesSerializationAndExpiredSource(t *testing.T) {
	runtime, err := Open(context.Background(), options(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	source, cancel := context.WithCancel(context.Background())
	source, span := runtime.Tracer("test").Start(source, "origin")
	source = requestmeta.Ensure(source)
	data, err := json.Marshal(Capture(source))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	span.End()
	var carrier Carrier
	if err = json.Unmarshal(data, &carrier); err != nil {
		t.Fatal(err)
	}
	worker, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	restored, err := Restore(worker, carrier)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Err() != nil || trace.SpanContextFromContext(restored).TraceID() != span.SpanContext().TraceID() || requestmeta.From(restored).RequestID != requestmeta.From(source).RequestID {
		t.Fatal("durable propagation lost identity or inherited request cancellation")
	}
	stop()
	if !errors.Is(restored.Err(), context.Canceled) {
		t.Fatal("worker cancellation lost")
	}
	for _, bad := range []Carrier{{"traceparent": "secret-malformed"}, {"tracestate": "without-parent"}, {"x-request-id": "secret-invalid"}, {"authorization": "never-propagate"}} {
		_, err = Restore(context.Background(), bad)
		if err == nil || strings.Contains(err.Error(), "secret-") {
			t.Fatal("invalid propagation accepted or leaked")
		}
	}
}

func TestUnsampledSpansStillCarryCanonicalIDs(t *testing.T) {
	o := options()
	o.Config.SampleRatio = 0
	runtime, err := Open(context.Background(), o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	ctx, span := runtime.Tracer("test").Start(context.Background(), "unsampled")
	defer span.End()
	if !span.SpanContext().IsValid() || span.IsRecording() || span.SpanContext().IsSampled() {
		t.Fatal("unexpected sampling behavior")
	}
	ctx = requestmeta.Ensure(ctx)
	if requestmeta.From(ctx).TraceID != span.SpanContext().TraceID().String() {
		t.Fatal("unsampled trace/log ID mismatch")
	}
}
