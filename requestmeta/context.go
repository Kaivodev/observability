// Package requestmeta carries correlation identifiers, never authorization proof.
package requestmeta

import (
	"context"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type Info struct {
	RequestID string
	TraceID   string
}

type contextKey struct{}

var tracePattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func New() Info {
	return Info{RequestID: uuid.NewString(), TraceID: strings.ReplaceAll(uuid.NewString(), "-", "")}
}

func (i Info) Valid() bool {
	id, err := uuid.Parse(i.RequestID)
	return err == nil && id != uuid.Nil && tracePattern.MatchString(i.TraceID) && strings.Trim(i.TraceID, "0") != ""
}

// With is for locally generated IDs or metadata from an authenticated internal
// peer. Public HTTP headers are not used to construct this context.
func With(ctx context.Context, info Info) context.Context {
	return context.WithValue(ctx, contextKey{}, info)
}

func From(ctx context.Context) Info {
	if ctx == nil {
		return Info{}
	}
	info, _ := ctx.Value(contextKey{}).(Info)
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		info.TraceID = span.TraceID().String()
	}
	return info
}

// Ensure keeps an existing correlation ID and uses the active span's trace ID.
func Ensure(ctx context.Context) context.Context {
	info := From(ctx)
	if !info.Valid() {
		fresh := New()
		if span := trace.SpanContextFromContext(ctx); span.IsValid() {
			fresh.TraceID = span.TraceID().String()
		}
		info = fresh
	}
	return With(ctx, info)
}
