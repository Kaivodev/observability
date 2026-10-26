package asynqobs

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// FrameworkLogger maps framework diagnostics to runtime.log. It has no task
// context; task errors are sanitized by Middleware before Asynq can persist them.
// Unknown free-form diagnostics are classified rather than copied into logs.
type FrameworkLogger struct{ log *slog.Logger }

func NewFrameworkLogger(log *slog.Logger) *FrameworkLogger {
	return &FrameworkLogger{log: log.With("component", "asynq")}
}
func (l *FrameworkLogger) write(level slog.Level, args ...any) {
	message := fmt.Sprint(args...)
	event := "asynq framework diagnostic"
	switch {
	case strings.HasPrefix(message, "Starting processing"):
		event = "asynq processing started"
	case strings.HasPrefix(message, "Starting graceful shutdown"):
		event = "asynq graceful shutdown started"
	case strings.HasPrefix(message, "Stopping processor"):
		event = "asynq processor stopping"
	case strings.HasPrefix(message, "Processor stopped"):
		event = "asynq processor stopped"
	case strings.HasPrefix(message, "Exiting"):
		event = "asynq stopped"
	case strings.HasPrefix(message, "Dequeue error:"):
		event = "asynq dequeue failed"
	case strings.HasPrefix(message, "Retry exhausted"):
		event = "asynq retry exhausted"
	case strings.HasPrefix(message, "Quitting worker"):
		event = "asynq worker interrupted"
	case strings.Contains(message, "heartbeat"):
		event = "asynq heartbeat diagnostic"
	case strings.Contains(message, "sync"):
		event = "asynq synchronization diagnostic"
	}
	l.log.Log(nil, level, event)
}
func (l *FrameworkLogger) Debug(args ...any) { l.write(slog.LevelDebug, args...) }
func (l *FrameworkLogger) Info(args ...any)  { l.write(slog.LevelInfo, args...) }
func (l *FrameworkLogger) Warn(args ...any)  { l.write(slog.LevelWarn, args...) }
func (l *FrameworkLogger) Error(args ...any) { l.write(slog.LevelError, args...) }
func (l *FrameworkLogger) Fatal(args ...any) { l.write(slog.LevelError, args...); os.Exit(1) }
