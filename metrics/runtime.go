package metrics

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaivodev/observability/internal/otlp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/credentials"
)

type Options struct {
	Config                     Config
	Service, Version, Instance string
}

type Runtime struct {
	provider        *sdkmetric.MeterProvider
	exporter        *safeExporter
	log             *slog.Logger
	shutdownTimeout time.Duration
	closeOnce       sync.Once
	warnOnce        sync.Once
	done            chan struct{}
	closeErr        error
}

// Open creates a local provider. The first export establishes the connection.
func Open(ctx context.Context, options Options, log *slog.Logger) (*Runtime, error) {
	cfg := options.Config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !otlp.ValidResourceValue(options.Service) || !otlp.ValidResourceValue(options.Instance) || log == nil {
		return nil, errors.New("metrics requires service, instance and runtime logger")
	}
	view := sdkmetric.NewView(sdkmetric.Instrument{Name: "*"}, sdkmetric.Stream{
		AttributeFilter: func(kv attribute.KeyValue) bool { return !Forbidden(string(kv.Key)) },
	})
	providerOptions := []sdkmetric.Option{
		sdkmetric.WithResource(otlp.Resource(options.Service, options.Version, options.Instance, cfg.Environment)),
		sdkmetric.WithCardinalityLimit(cfg.CardinalityLimit),
		sdkmetric.WithView(view),
		// The SDK otherwise preserves filtered measurement attributes on exemplars.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	}
	rt := &Runtime{log: log.With("component", "metrics"), shutdownTimeout: cfg.ShutdownTimeout, done: make(chan struct{})}
	if cfg.Endpoint != "" {
		exporter, err := newExporter(ctx, cfg)
		if err != nil {
			return nil, err
		}
		rt.exporter = &safeExporter{Exporter: exporter, log: rt.log}
		reader := sdkmetric.NewPeriodicReader(rt.exporter, sdkmetric.WithInterval(cfg.Interval), sdkmetric.WithTimeout(cfg.ExportTimeout))
		providerOptions = append(providerOptions, sdkmetric.WithReader(reader))
	}
	rt.provider = sdkmetric.NewMeterProvider(providerOptions...)
	return rt, nil
}

func newExporter(ctx context.Context, cfg Config) (*otlpmetricgrpc.Exporter, error) {
	tlsConfig, err := cfg.transport().TLSConfig("metrics")
	if err != nil {
		return nil, err
	}
	opts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpointURL(cfg.Endpoint),
		otlpmetricgrpc.WithTimeout(cfg.ExportTimeout),
		otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{Enabled: false}),
		otlpmetricgrpc.WithTemporalitySelector(sdkmetric.CumulativeTemporalitySelector),
	}
	if tlsConfig == nil {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	} else {
		opts = append(opts, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig)))
	}
	exp, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, errors.New("initialize OTLP metrics exporter failed")
	}
	return exp, nil
}

func (r *Runtime) Provider() metric.MeterProvider  { return r.provider }
func (r *Runtime) Meter(scope string) metric.Meter { return r.provider.Meter(scope) }

func (r *Runtime) ExportFailures() uint64 {
	if r.exporter == nil {
		return 0
	}
	return r.exporter.failures.Load()
}

var errShutdown = errors.New("metrics shutdown exceeded its budget or failed")

// Shutdown flushes once. Each caller can stop waiting at its own deadline,
// even if an SDK observation callback is slow to respond to cancellation.
func (r *Runtime) Shutdown(parent context.Context) error {
	// Preserve the previous result even if a later caller is already canceled.
	select {
	case <-r.done:
		return r.closeErr
	default:
	}
	ctx, cancel := context.WithTimeout(parent, r.shutdownTimeout)
	defer cancel()
	r.closeOnce.Do(func() {
		go func() {
			if err := r.provider.Shutdown(ctx); err != nil || ctx.Err() != nil {
				r.closeErr = errShutdown
			}
			close(r.done)
		}()
	})
	select {
	case <-r.done:
		if r.closeErr == nil {
			return nil
		}
	case <-ctx.Done():
	}
	r.warnOnce.Do(func() { r.log.Warn("metrics shutdown incomplete") })
	return errShutdown
}

// The periodic reader forwards exporter errors to the process-global handler.
// Consume them here so collector responses cannot enter application logs.
type safeExporter struct {
	sdkmetric.Exporter
	log      *slog.Logger
	failures atomic.Uint64
	failed   atomic.Bool
}

func (e *safeExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if err := e.Exporter.Export(ctx, rm); err != nil {
		e.failures.Add(1)
		if !e.failed.Swap(true) {
			e.log.Warn("metrics batch export failed")
		}
		return nil
	}
	if e.failed.Swap(false) {
		e.log.Info("metrics export recovered")
	}
	return nil
}
