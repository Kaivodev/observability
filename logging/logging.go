package logging

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"

	"github.com/aipay-dev/observability/redact"
	"github.com/aipay-dev/observability/requestmeta"
	"go.opentelemetry.io/otel/trace"
)

func New(out io.Writer, level slog.Level) *slog.Logger {
	return newLogger(out, level)
}

func newLogger(out io.Writer, level slog.Leveler) *slog.Logger {
	return slog.New(contextHandler{slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level, ReplaceAttr: replaceAttr})})
}

func replaceAttr(groups []string, attr slog.Attr) slog.Attr {
	for _, group := range groups {
		if redact.Sensitive(group) {
			return slog.String(attr.Key, redact.Mask)
		}
	}
	if redact.Sensitive(attr.Key) {
		return slog.String(attr.Key, redact.Mask)
	}
	if attr.Key == slog.LevelKey {
		if level, ok := attr.Value.Any().(slog.Level); ok {
			return slog.String(attr.Key, level.String())
		}
	}
	if attr.Key == slog.TimeKey && attr.Value.Kind() == slog.KindTime {
		return slog.Time(attr.Key, attr.Value.Time().UTC())
	}
	if attr.Value.Kind() == slog.KindAny {
		if _, ok := attr.Value.Any().(error); ok {
			return attr
		}
		data, err := redact.JSON(attr.Value.Any())
		if err != nil {
			return slog.String(attr.Key, "[unserializable]")
		}
		return slog.Any(attr.Key, json.RawMessage(data))
	}
	return attr
}

type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	metadata := requestmeta.From(ctx)
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		clean := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
		record.Attrs(func(a slog.Attr) bool {
			if a.Key != "trace_id" && a.Key != "span_id" && a.Key != "trace_sampled" {
				clean.AddAttrs(a)
			}
			return true
		})
		clean.AddAttrs(slog.String("trace_id", span.TraceID().String()), slog.String("span_id", span.SpanID().String()), slog.Bool("trace_sampled", span.IsSampled()))
		record = clean
	}
	if metadata.Valid() {
		hasRequest, hasTrace := false, false
		record.Attrs(func(a slog.Attr) bool {
			hasRequest = hasRequest || a.Key == "request_id"
			hasTrace = hasTrace || a.Key == "trace_id"
			return true
		})
		if !hasRequest {
			record.AddAttrs(slog.String("request_id", metadata.RequestID))
		}
		if !hasTrace {
			record.AddAttrs(slog.String("trace_id", metadata.TraceID))
		}
	}
	return h.Handler.Handle(ctx, record)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	filtered := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if a.Key != "trace_id" && a.Key != "span_id" && a.Key != "trace_sampled" {
			filtered = append(filtered, a)
		}
	}
	return contextHandler{h.Handler.WithAttrs(filtered)}
}
func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}
