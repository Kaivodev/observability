package tracing

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
)

type Options struct {
	Config                     Config
	Service, Version, Instance string
}
type Runtime struct {
	provider  *sdktrace.TracerProvider
	config    Config
	log       *slog.Logger
	exporter  *safeExporter
	closeOnce sync.Once
	closeErr  error
}

// Open is nonblocking with respect to the collector. Application readiness never
// depends on telemetry availability. Configuration/certificate errors fail early.
func Open(ctx context.Context, options Options, log *slog.Logger) (*Runtime, error) {
	if err := options.Config.Validate(); err != nil {
		return nil, err
	}
	if !validResource(options.Service) || !validResource(options.Instance) || log == nil {
		return nil, errors.New("tracing requires service, instance and runtime logger")
	}
	resource := resource.NewSchemaless(attribute.String("service.name", options.Service), attribute.String("service.version", options.Version), attribute.String("service.instance.id", options.Instance), attribute.String("deployment.environment.name", options.Config.Environment))
	rt := &Runtime{config: options.Config, log: log}
	providerOptions := []sdktrace.TracerProviderOption{sdktrace.WithResource(resource), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(options.Config.SampleRatio)))}
	if options.Config.Endpoint != "" {
		exporterOptions, err := exportOptions(options.Config)
		if err != nil {
			return nil, err
		}
		exporter, err := otlptracegrpc.New(ctx, exporterOptions...)
		if err != nil {
			return nil, errors.New("initialize OTLP trace exporter failed")
		}
		rt.exporter = &safeExporter{next: exporter, log: log.With("component", "tracing")}
		providerOptions = append(providerOptions, sdktrace.WithBatcher(rt.exporter, sdktrace.WithMaxQueueSize(options.Config.QueueSize), sdktrace.WithMaxExportBatchSize(options.Config.BatchSize), sdktrace.WithBatchTimeout(options.Config.BatchInterval), sdktrace.WithExportTimeout(options.Config.ExportTimeout)))
	}
	rt.provider = sdktrace.NewTracerProvider(providerOptions...)
	return rt, nil
}

func (r *Runtime) Provider() trace.TracerProvider            { return r.provider }
func (r *Runtime) Tracer(scope string) trace.Tracer          { return r.provider.Tracer(scope) }
func (r *Runtime) Propagator() propagation.TextMapPropagator { return propagation.TraceContext{} }
func (r *Runtime) ExportFailures() uint64 {
	if r.exporter == nil {
		return 0
	}
	return r.exporter.failures.Load()
}

// Shutdown shares the caller's remaining application drain budget and also caps
// its own flush duration. Export failure is diagnostic, not a business failure.
func (r *Runtime) Shutdown(parent context.Context) error {
	r.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(parent, r.config.ShutdownTimeout)
		defer cancel()
		if err := r.provider.Shutdown(ctx); err != nil {
			r.closeErr = errors.New("trace shutdown exceeded its budget or failed")
			r.log.Warn("trace shutdown incomplete", "component", "tracing")
		}
	})
	return r.closeErr
}

func validResource(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < ' ' || r == 127 {
			return false
		}
	}
	return true
}

func exportOptions(c Config) ([]otlptracegrpc.Option, error) {
	u, _ := url.Parse(c.Endpoint)
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(c.Endpoint), otlptracegrpc.WithTimeout(c.ExportTimeout)}
	if u.Scheme == "http" {
		return append(opts, otlptracegrpc.WithInsecure()), nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: u.Hostname()}
	if c.CAFile != "" {
		data, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, errors.New("read trace CA bundle failed")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, errors.New("trace CA bundle contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if c.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, errors.New("load trace client certificate failed")
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	return append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(tlsConfig))), nil
}

// The SDK invokes its process-global error handler for returned exporter errors.
// Handle these locally instead, preserving per-service logging and withholding
// collector error messages which may contain credentials or rejected span data.
// Failed batches are explicitly counted and reported; there is no durable spool.
type safeExporter struct {
	next     sdktrace.SpanExporter
	log      *slog.Logger
	failures atomic.Uint64
	failed   atomic.Bool
}

func (e *safeExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.next.ExportSpans(ctx, spans); err != nil {
		e.failures.Add(1)
		if !e.failed.Swap(true) {
			e.log.Warn("trace batch export failed", "span_count", len(spans))
		}
		return nil
	}
	if e.failed.Swap(false) {
		e.log.Info("trace export recovered")
	}
	return nil
}
func (e *safeExporter) Shutdown(ctx context.Context) error {
	if err := e.next.Shutdown(ctx); err != nil {
		return errors.New("OTLP exporter shutdown failed")
	}
	return nil
}
