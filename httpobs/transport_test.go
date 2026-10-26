package httpobs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPPropagationPrivacyAndResponseLifetime(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{false: "external", true: "internal"}[trusted], func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer provider.Shutdown(context.Background())
			received := make(chan http.Header, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received <- r.Header.Clone()
				w.WriteHeader(502)
				_, _ = io.WriteString(w, "response-secret")
			}))
			defer server.Close()
			ctx, parent := provider.Tracer("test").Start(context.Background(), "owner")
			defer parent.End()
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/private-path?token=query-secret", nil)
			request.Header.Set("traceparent", "untrusted-original")
			request.Header.Set("baggage", "token=baggage-secret")
			client := &http.Client{Transport: &Transport{Tracer: provider.Tracer("test"), Operation: "provider.query", Propagate: trusted}}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			if len(recorder.Ended()) != 0 {
				t.Fatal("span ended before response consumption")
			}
			_, err = io.Copy(io.Discard, response.Body)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			spans := recorder.Ended()
			if len(spans) != 1 {
				t.Fatalf("expected exactly one client span: %d", len(spans))
			}
			span := spans[0]
			if span.Parent().SpanID() != parent.SpanContext().SpanID() || span.SpanKind() != trace.SpanKindClient || span.Status().Code != codes.Error {
				t.Fatal("incorrect client parent, kind or HTTP error outcome")
			}
			headers := <-received
			remote := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(headers)))
			if trusted && remote.SpanID() != span.SpanContext().SpanID() {
				t.Fatal("trusted HTTP propagation does not match client span")
			}
			if !trusted && headers.Get("traceparent") != "" {
				t.Fatal("external call leaked trace context")
			}
			if headers.Get("baggage") != "" || request.Header.Get("traceparent") != "untrusted-original" {
				t.Fatal("baggage leaked or original request mutated")
			}
			for _, attr := range span.Attributes() {
				if strings.Contains(attr.Value.Emit(), "secret") || strings.Contains(attr.Value.Emit(), "private-path") {
					t.Fatal("HTTP span contains request data")
				}
			}
		})
	}
}

func TestHTTPCancellationEndsClientSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	client := &http.Client{Transport: &Transport{Tracer: provider.Tracer("test"), Operation: "provider.query"}}
	if _, err := client.Do(request); err == nil {
		t.Fatal("canceled request succeeded")
	}
	if spans := recorder.Ended(); len(spans) != 1 || spans[0].Status().Code != codes.Error {
		t.Fatal("transport error did not end an error span")
	}
}
