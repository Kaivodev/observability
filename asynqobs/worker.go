package asynqobs

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aipay-dev/observability/requestmeta"
	"github.com/aipay-dev/observability/tracing"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Worker struct {
	tracer          trace.Tracer
	access, runtime *slog.Logger
	maxPayload      int
}

type attemptKey struct{}
type attemptState struct {
	value atomic.Pointer[attemptMetadata]
}
type attemptMetadata struct {
	ids       requestmeta.Info
	span      trace.SpanContext
	execution string
}

// BaseContext must be called once per task by asynq.Config.BaseContext. The
// shared cell lets ErrorHandler recover the attempt when Asynq itself selects
// a deadline or lost-lease error instead of the handler's returned error.
func (w *Worker) BaseContext(parent context.Context) context.Context {
	return context.WithValue(parent, attemptKey{}, new(attemptState))
}

func NewWorker(provider trace.TracerProvider, access, runtime *slog.Logger, maxPayload int) (*Worker, error) {
	if provider == nil || access == nil || runtime == nil || maxPayload < 1 || maxPayload > 16<<20 {
		return nil, errors.New("invalid Asynq worker observation options")
	}
	return &Worker{tracer: provider.Tracer("github.com/aipay-dev/observability/asynqobs"), access: access, runtime: runtime, maxPayload: maxPayload}, nil
}

func (w *Worker) Middleware(next asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) (err error) {
		if task == nil || next == nil {
			return errors.Join(asynq.SkipRetry, errors.New("invalid task handler"))
		}
		ctx = requestmeta.Ensure(ctx)
		carrier := tracing.Carrier{}
		var invalid error
		for key, value := range task.Headers() {
			if key != traceModeHeader {
				carrier[key] = value
			}
		}
		if !taskTypePattern.MatchString(task.Type()) || len(task.Payload()) > w.maxPayload {
			invalid = errors.New("invalid task type or payload size")
		}
		if len(task.Headers()) > 4 {
			invalid = errors.New("invalid task header count")
		}
		mode := task.Headers()[traceModeHeader]
		if mode != "" && mode != "link" {
			invalid = errors.New("invalid task trace mode")
		}
		if invalid == nil {
			var restoreErr error
			ctx, restoreErr = tracing.Restore(ctx, carrier)
			if restoreErr != nil {
				invalid = errors.New("invalid task propagation metadata")
			}
		}
		source := trace.SpanContextFromContext(ctx)
		name := "process invalid-task"
		if taskTypePattern.MatchString(task.Type()) {
			name = "process " + task.Type()
		}
		startOptions := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("messaging.system", "asynq"), attribute.String("messaging.operation.type", "process"))}
		if mode == "link" || carrier["traceparent"] == "" {
			startOptions = append(startOptions, trace.WithNewRoot())
			if mode == "link" && source.IsValid() {
				startOptions = append(startOptions, trace.WithLinks(trace.Link{SpanContext: source}))
			}
		}
		var span trace.Span
		ctx, span = w.tracer.Start(ctx, name, startOptions...)
		ctx = requestmeta.Ensure(ctx)
		taskID, _ := asynq.GetTaskID(ctx)
		queue, _ := asynq.GetQueueName(ctx)
		retry, _ := asynq.GetRetryCount(ctx)
		maxRetry, _ := asynq.GetMaxRetry(ctx)
		execution := uuid.NewString()
		if state, ok := ctx.Value(attemptKey{}).(*attemptState); ok {
			state.value.Store(&attemptMetadata{ids: requestmeta.From(ctx), span: span.SpanContext(), execution: execution})
		}
		span.SetAttributes(attribute.String("messaging.message.id", taskID), attribute.String("messaging.destination.name", queue), attribute.Int("aipay.task.retry_count", retry), attribute.String("aipay.task.execution_id", execution))
		started := time.Now()
		var finishOnce sync.Once
		finish := func(result error, panicOccurred bool) {
			finishOnce.Do(func() {
				defer span.End()
				code := ""
				outcome := "succeeded"
				level := slog.LevelInfo
				if result != nil {
					code = errorCode(result)
					if panicOccurred {
						code = "task_panic"
					}
					if invalid != nil {
						code = "invalid_task"
					}
					outcome = "failed"
					level = slog.LevelWarn
					span.SetStatus(codes.Error, code)
				}
				fields := []any{"protocol", "asynq", "operation", "process", "task_id", taskID, "queue", queue, "execution_id", execution, "retry_count", retry, "max_retry", maxRetry, "outcome", outcome, "failed", result != nil, "duration_ms", time.Since(started).Milliseconds()}
				if mode == "link" && source.IsValid() {
					fields = append(fields, "origin_trace_id", source.TraceID().String())
				}
				if invalid == nil {
					fields = append(fields, "task_type", task.Type())
					span.SetAttributes(attribute.String("aipay.task.type", task.Type()))
				}
				if code != "" {
					fields = append(fields, "error_code", code)
				}
				w.access.Log(ctx, level, "queue task execution", fields...)
			})
		}
		// A handler may ignore cancellation. End the observable attempt at its
		// deadline even while owner code still has to stop its own goroutines.
		stop := context.AfterFunc(ctx, func() { finish(ctx.Err(), false) })
		defer func() {
			panicOccurred := false
			if recover() != nil {
				panicOccurred = true
				err = errors.New("task handler panicked")
				w.runtime.ErrorContext(ctx, "task handler panic", "task_id", taskID, "execution_id", execution, "stack", string(debug.Stack()))
			}
			stop()
			if err == nil {
				err = ctx.Err()
			}
			finish(err, panicOccurred)
			if err != nil {
				code := errorCode(err)
				if panicOccurred {
					code = "task_panic"
				}
				if invalid != nil {
					code = "invalid_task"
				}
				err = wrapFailure(ctx, err, code)
			}
		}()
		if invalid != nil {
			return errors.Join(asynq.SkipRetry, invalid)
		}
		return next.ProcessTask(ctx, task)
	})
}

// HandleError logs exceptional failures, not a second copy of each access event.
// It restores the exact attempt context from our safe error wrapper because
// Asynq invokes ErrorHandler with the context outside its handler middleware.
func (w *Worker) HandleError(ctx context.Context, task *asynq.Task, err error) {
	var failure *failure
	if errors.As(err, &failure) {
		ctx = requestmeta.With(trace.ContextWithSpanContext(ctx, failure.span), failure.ids)
	} else if state, ok := ctx.Value(attemptKey{}).(*attemptState); ok {
		if metadata := state.value.Load(); metadata != nil {
			ctx = requestmeta.With(trace.ContextWithSpanContext(ctx, metadata.span), metadata.ids)
		}
	}
	retry, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if !errors.Is(err, asynq.SkipRetry) && !errors.Is(err, asynq.RevokeTask) && retry < maxRetry && errorCode(err) != "task_panic" {
		return
	}
	id, _ := asynq.GetTaskID(ctx)
	// This is a retry decision, not confirmation that Redis archived the task.
	w.runtime.WarnContext(ctx, "task requires failure handling", "task_id", id, "retry_count", retry, "max_retry", maxRetry, "error_code", errorCode(err))
}
