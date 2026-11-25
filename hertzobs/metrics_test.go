package hertzobs

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestHTTPMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	observe, err := Metrics(provider, "public_api")
	if err != nil {
		t.Fatal(err)
	}
	serve := func(method, uri, route string, status int) {
		c := app.NewContext(0)
		c.Request.SetRequestURI(uri)
		c.Request.Header.SetMethod(method)
		c.Request.Header.Set("X-Request-ID", "arbitrary")
		c.SetFullPath(route)
		c.SetHandlers(app.HandlersChain{observe, func(_ context.Context, c *app.RequestContext) {
			c.SetStatusCode(status)
		}})
		c.Next(context.Background())
	}
	serve("GET", "/orders/first?token=one", "/orders/:order", 202)
	serve("GET", "/orders/second?token=two", "/orders/:order", 202)
	serve("CUSTOM", "/private/path?code=secret", "", 404)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) != 1 || len(rm.ScopeMetrics[0].Metrics) != 1 {
		t.Fatal("expected one histogram")
	}
	m := rm.ScopeMetrics[0].Metrics[0]
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok || m.Name != "kaivo.http.server.duration" || m.Unit != "s" || len(h.DataPoints) != 2 {
		t.Fatalf("unexpected HTTP metric %s (%s), %d series", m.Name, m.Unit, len(h.DataPoints))
	}
	attrs := func(method, route, status string) attribute.Set {
		return attribute.NewSet(attribute.String("listener", "public_api"), attribute.String("http_method", method),
			attribute.String("http_route", route), attribute.String("http_status_code", status))
	}
	counts := map[attribute.Set]uint64{attrs("GET", "/orders/:order", "202"): 2, attrs("_OTHER", "unmatched", "404"): 1}
	for _, point := range h.DataPoints {
		if count, ok := counts[point.Attributes]; !ok || point.Count != count {
			t.Fatalf("unexpected HTTP series: %v (%d)", point.Attributes, point.Count)
		}
		delete(counts, point.Attributes)
		if !slices.Equal(point.Bounds, []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}) || point.Sum <= 0 {
			t.Fatal("incorrect duration or bucket boundaries")
		}
	}
}

func TestHTTPMetricsValidation(t *testing.T) {
	if _, err := Metrics(nil, "public"); err == nil {
		t.Fatal("accepted nil provider")
	}
	for _, name := range []string{"", "1api", "_api", "Public", "private-port", "api\n", strings.Repeat("a", 33)} {
		if _, err := Metrics(noop.NewMeterProvider(), name); err == nil {
			t.Errorf("accepted listener %q", name)
		}
	}
	for _, name := range []string{"a", "public_2", strings.Repeat("a", 32)} {
		if _, err := Metrics(noop.NewMeterProvider(), name); err != nil {
			t.Error(err)
		}
	}
}
