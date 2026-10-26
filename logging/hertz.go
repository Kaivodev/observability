package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/cloudwego/hertz/pkg/common/hlog"
)

// frameworkLogger keeps Hertz lifecycle diagnostics in the structured runtime file.
// Configure the global Hertz logger before starting server goroutines.
type frameworkLogger struct {
	current atomic.Pointer[slog.Logger]
	level   slog.LevelVar
	attrs   []any
}

var _ hlog.FullLogger = (*frameworkLogger)(nil)

func newFrameworkLogger(out io.Writer, level slog.Level, attrs []any) *frameworkLogger {
	logger := &frameworkLogger{attrs: append([]any(nil), attrs...)}
	logger.level.Set(level)
	logger.SetOutput(out)
	return logger
}

func (l *frameworkLogger) SetOutput(out io.Writer) {
	l.current.Store(newLogger(out, &l.level).With(l.attrs...))
}

func (l *frameworkLogger) SetLevel(level hlog.Level) {
	levels := [...]slog.Level{-8, -4, 0, 2, 4, 8, 12}
	if int(level) < 0 || int(level) >= len(levels) {
		return
	}
	l.level.Set(levels[level])
}

func (l *frameworkLogger) Trace(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(-8), fmt.Sprint(values...))
}
func (l *frameworkLogger) Tracef(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(-8), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxTracef(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(-8), fmt.Sprintf(format, values...))
}

func (l *frameworkLogger) Debug(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(-4), fmt.Sprint(values...))
}
func (l *frameworkLogger) Debugf(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(-4), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxDebugf(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(-4), fmt.Sprintf(format, values...))
}

func (l *frameworkLogger) Info(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(0), fmt.Sprint(values...))
}
func (l *frameworkLogger) Infof(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(0), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxInfof(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(0), fmt.Sprintf(format, values...))
}

func (l *frameworkLogger) Notice(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(2), fmt.Sprint(values...))
}
func (l *frameworkLogger) Noticef(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(2), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxNoticef(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(2), fmt.Sprintf(format, values...))
}

func (l *frameworkLogger) Warn(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(4), fmt.Sprint(values...))
}
func (l *frameworkLogger) Warnf(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(4), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxWarnf(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(4), fmt.Sprintf(format, values...))
}

func (l *frameworkLogger) Error(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(8), fmt.Sprint(values...))
}
func (l *frameworkLogger) Errorf(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(8), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxErrorf(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(8), fmt.Sprintf(format, values...))
}

func (l *frameworkLogger) Fatal(values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(12), fmt.Sprint(values...))
}
func (l *frameworkLogger) Fatalf(format string, values ...any) {
	l.current.Load().Log(context.Background(), slog.Level(12), fmt.Sprintf(format, values...))
}
func (l *frameworkLogger) CtxFatalf(ctx context.Context, format string, values ...any) {
	l.current.Load().Log(ctx, slog.Level(12), fmt.Sprintf(format, values...))
}
