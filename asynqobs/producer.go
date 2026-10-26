// Package asynqobs instruments queue admission and each worker execution.
// It preserves Asynq retry signals, never logs payloads, and owns no business SQL.
package asynqobs

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"time"

	"github.com/aipay-dev/observability/requestmeta"
	"github.com/aipay-dev/observability/tracing"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const traceModeHeader = "aipay-trace-mode"

var taskTypePattern = regexp.MustCompile(`^[a-z][a-zA-Z0-9_.:-]{0,127}$`)

// Message is transport metadata suitable for service-owned outbox storage.
// The owning service supplies a stable asynq.TaskID when dispatching an outbox
// row. Payload bytes remain the owning service's versioned protobuf contract.
type Message struct {
	Type        string          `json:"type"`
	Payload     []byte          `json:"payload"`
	Propagation tracing.Carrier `json:"propagation"`
	NewTrace    bool            `json:"new_trace,omitempty"`
}

func NewMessage(ctx context.Context, kind string, payload []byte) Message {
	return Message{Type: kind, Payload: append([]byte(nil), payload...), Propagation: tracing.Capture(ctx)}
}

type Enqueuer interface {
	EnqueueContext(context.Context, *asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error)
}
type Producer struct {
	client     Enqueuer
	tracer     trace.Tracer
	access     *slog.Logger
	maxPayload int
}

func NewProducer(client Enqueuer, provider trace.TracerProvider, access *slog.Logger, maxPayload int) (*Producer, error) {
	if client == nil || provider == nil || access == nil || maxPayload < 1 || maxPayload > 16<<20 {
		return nil, errors.New("invalid Asynq producer options")
	}
	return &Producer{client: client, tracer: provider.Tracer("github.com/aipay-dev/observability/asynqobs"), access: access, maxPayload: maxPayload}, nil
}

func (p *Producer) Enqueue(ctx context.Context, message Message, options ...asynq.Option) (info *asynq.TaskInfo, err error) {
	if !taskTypePattern.MatchString(message.Type) || len(message.Payload) > p.maxPayload {
		return nil, errors.New("invalid task type or payload size")
	}
	if len(message.Propagation) > 0 {
		ctx, err = tracing.Restore(ctx, message.Propagation)
		if err != nil {
			return nil, errors.New("invalid task propagation metadata")
		}
	}
	ctx = requestmeta.Ensure(ctx)
	ctx, span := p.tracer.Start(ctx, "enqueue "+message.Type, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("messaging.system", "asynq"), attribute.String("messaging.operation.type", "send"), attribute.String("aipay.task.type", message.Type)))
	defer span.End()
	started := time.Now()
	defer func() {
		level := slog.LevelInfo
		outcome := "accepted"
		code := ""
		if err != nil {
			level = slog.LevelWarn
			outcome = "rejected"
			code = errorCode(err)
			span.SetStatus(codes.Error, code)
			err = wrapFailure(ctx, err, code)
		}
		fields := []any{"protocol", "asynq", "operation", "enqueue", "task_type", message.Type, "outcome", outcome, "failed", err != nil, "duration_ms", time.Since(started).Milliseconds()}
		if code != "" {
			fields = append(fields, "error_code", code)
		}
		if info != nil {
			fields = append(fields, "task_id", info.ID, "queue", info.Queue)
			span.SetAttributes(attribute.String("messaging.message.id", info.ID), attribute.String("messaging.destination.name", info.Queue))
		}
		p.access.Log(ctx, level, "queue admission", fields...)
	}()
	headers := map[string]string(tracing.Capture(ctx))
	if message.NewTrace {
		headers[traceModeHeader] = "link"
	}
	task := asynq.NewTaskWithHeaders(message.Type, append([]byte(nil), message.Payload...), headers, options...)
	info, err = p.client.EnqueueContext(ctx, task)
	if info == nil && err == nil {
		err = errors.New("queue client returned no task admission result")
	}
	return info, err
}
