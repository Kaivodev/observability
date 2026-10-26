// Package hertzobs instruments public Hertz requests without trusting public
// trace headers or recording raw URLs, queries, headers or request bodies.
package hertzobs

import (
	"context"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Result struct {
	Failed   bool
	Code     int64
	Protocol string
}
type Outcome func(*app.RequestContext) Result

// Middleware must run outside request-ID, access-log and recovery middleware.
// The outcome callback lets protocol-specific HTTP-200 failures mark the span.
func Middleware(provider trace.TracerProvider, outcome Outcome) app.HandlerFunc {
	tracer := provider.Tracer("github.com/aipay-dev/observability/hertzobs")
	return func(ctx context.Context, c *app.RequestContext) {
		ctx, span := tracer.Start(ctx, "HTTP request", trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		c.Next(ctx)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := string(c.Method())
		switch method {
		case "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH":
		default:
			method = "_OTHER"
		}
		span.SetName(method + " " + route)
		span.SetAttributes(attribute.String("http.request.method", method), attribute.String("http.route", route), attribute.Int("http.response.status_code", c.Response.StatusCode()))
		result := Result{Failed: c.Response.StatusCode() >= 400, Code: int64(c.Response.StatusCode()), Protocol: "http"}
		if outcome != nil {
			result = outcome(c)
		}
		span.SetAttributes(attribute.String("aipay.protocol", result.Protocol), attribute.String("aipay.result.code", strconv.FormatInt(result.Code, 10)))
		if result.Failed {
			span.SetStatus(codes.Error, "request_failed")
		}
	}
}
