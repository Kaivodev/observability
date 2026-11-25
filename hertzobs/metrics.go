package hertzobs

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var listenerName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// Metrics records server latency for a configured listener. Install it outside
// recovery middleware so the histogram includes the final response status.
func Metrics(provider metric.MeterProvider, listener string) (app.HandlerFunc, error) {
	if provider == nil {
		return nil, errors.New("HTTP metrics require a meter provider")
	}
	if !listenerName.MatchString(listener) {
		return nil, errors.New("invalid HTTP metrics listener name")
	}
	duration, err := provider.Meter(scope).Float64Histogram("kaivo.http.server.duration",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, errors.New("initialize HTTP duration histogram failed")
	}
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		elapsed := time.Since(start).Seconds()
		method, route := methodAndRoute(c)
		duration.Record(ctx, elapsed, metric.WithAttributes(
			attribute.String("listener", listener),
			attribute.String("http_route", route),
			attribute.String("http_method", method),
			attribute.String("http_status_code", strconv.Itoa(c.Response.StatusCode())),
		))
	}, nil
}
