package tracing

import (
	"context"
	"errors"

	"github.com/Kaivodev/observability/requestmeta"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Carrier contains only serializable W3C trace context and a correlation ID.
// Persist it alongside an outbox record, never a context.Context or its deadline.
type Carrier map[string]string

func Capture(ctx context.Context) Carrier {
	result := Carrier{}
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(result))
	ids := requestmeta.From(ctx)
	if !ids.Valid() {
		ids = requestmeta.New()
	}
	result["x-request-id"] = ids.RequestID
	return result
}

// Restore preserves the worker's cancellation/deadline. Call only for metadata
// from an authenticated internal transport or a service-owned outbox/queue.
func Restore(parent context.Context, carrier Carrier) (context.Context, error) {
	if len(carrier) > 3 {
		return parent, errors.New("invalid trace carrier fields")
	}
	for key, value := range carrier {
		if key != "traceparent" && key != "tracestate" && key != "x-request-id" {
			return parent, errors.New("unsupported trace carrier field")
		}
		if len(value) > 512 {
			return parent, errors.New("trace carrier value exceeds limit")
		}
	}
	var source trace.SpanContext
	if carrier["traceparent"] != "" {
		parsed := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(carrier))
		source = trace.SpanContextFromContext(parsed)
		if !source.IsValid() {
			return parent, errors.New("invalid traceparent")
		}
		// Reject malformed tracestate instead of silently dropping part of a carrier.
		if _, err := trace.ParseTraceState(carrier["tracestate"]); err != nil {
			return parent, errors.New("invalid tracestate")
		}

	} else if carrier["tracestate"] != "" {
		return parent, errors.New("tracestate requires traceparent")
	}
	ids := requestmeta.New()
	if carrier["x-request-id"] != "" {
		id, err := uuid.Parse(carrier["x-request-id"])
		if err != nil || id == uuid.Nil {
			return parent, errors.New("invalid request correlation ID")
		}
		ids.RequestID = id.String()
	}
	if source.IsValid() {
		ids.TraceID = source.TraceID().String()
		parent = trace.ContextWithRemoteSpanContext(parent, source)
	}
	return requestmeta.With(parent, ids), nil
}
