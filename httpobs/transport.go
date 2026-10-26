// Package httpobs traces outbound HTTP without recording URLs, queries, headers
// or bodies. Trace-header propagation is opt-in for trusted internal endpoints.
package httpobs

import (
	"errors"
	"io"
	"net/http"
	"regexp"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var operationName = regexp.MustCompile(`^[a-z][a-z0-9_.:-]{0,127}$`)

type Transport struct {
	Base      http.RoundTripper
	Tracer    trace.Tracer
	Operation string
	Propagate bool
}

func (t *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.Tracer == nil || !operationName.MatchString(t.Operation) {
		return nil, errors.New("HTTP tracing requires a tracer and stable operation name")
	}
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	ctx, span := t.Tracer.Start(request.Context(), t.Operation, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("http.request.method", request.Method)))
	clone := request.Clone(ctx)
	clone.Header.Del("traceparent")
	clone.Header.Del("tracestate")
	clone.Header.Del("baggage")
	if t.Propagate {
		propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(clone.Header))
	}
	response, err := base.RoundTrip(clone)
	if err != nil {
		span.SetStatus(codes.Error, "http_transport_failed")
		span.End()
		return response, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", response.StatusCode))
	if response.StatusCode >= 400 {
		span.SetStatus(codes.Error, "http_response_failed")
	}
	if response.Body == nil {
		span.End()
	} else {
		response.Body = &body{ReadCloser: response.Body, span: span}
	}
	return response, nil
}

type body struct {
	io.ReadCloser
	span trace.Span
	once sync.Once
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		if err != io.EOF {
			b.span.SetStatus(codes.Error, "http_body_read_failed")
		}
		b.once.Do(func() { b.span.End() })
	}
	return n, err
}
func (b *body) Close() error {
	err := b.ReadCloser.Close()
	if err != nil {
		b.span.SetStatus(codes.Error, "http_body_close_failed")
	}
	b.once.Do(func() { b.span.End() })
	return err
}
