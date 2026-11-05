package asynqobs

import (
	"context"
	"errors"

	"github.com/Kaivodev/observability/requestmeta"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel/trace"
)

type failure struct {
	cause error
	code  string
	ids   requestmeta.Info
	span  trace.SpanContext
}

func (f *failure) Error() string { return "asynq operation failed: " + f.code }
func (f *failure) Unwrap() error { return f.cause }
func wrapFailure(ctx context.Context, err error, code string) error {
	return &failure{cause: err, code: code, ids: requestmeta.From(ctx), span: trace.SpanContextFromContext(ctx)}
}
func errorCode(err error) string {
	var observed *failure
	if errors.As(err, &observed) {
		return observed.code
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, asynq.RevokeTask):
		return "task_revoked"
	case errors.Is(err, asynq.SkipRetry):
		return "retry_skipped"
	case errors.Is(err, asynq.ErrTaskIDConflict), errors.Is(err, asynq.ErrDuplicateTask):
		return "duplicate_task"
	default:
		return "task_failed"
	}
}
