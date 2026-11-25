package logging

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Files struct {
	InstanceID string
	Access     *slog.Logger
	Runtime    *slog.Logger
	Framework  *frameworkLogger
	access     *logWriter
	runtime    *logWriter
}

// Open uses two rolling files or one shared stdout writer, according to cfg.Output.
func Open(cfg Options, level slog.Level) (*Files, error) {
	return openWithOutput(cfg, level, os.Stdout, os.Stderr)
}

func openWithOutput(cfg Options, level slog.Level, stdout, stderr io.Writer) (*Files, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Output == "stdout" {
		shared := &logWriter{out: stdout, fallback: stderr}
		return newFiles(cfg, level, shared, shared), nil
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	access, err := openWriter(filepath.Join(cfg.Dir, "access.log"), cfg)
	if err != nil {
		return nil, err
	}
	runtime, err := openWriter(filepath.Join(cfg.Dir, "runtime.log"), cfg)
	if err != nil {
		_ = access.Close()
		return nil, err
	}
	return newFiles(cfg, level, access, runtime), nil
}

func newFiles(cfg Options, level slog.Level, access, runtime *logWriter) *Files {
	instance := cfg.InstanceID
	if instance == "" {
		instance = uuid.NewString()
	}
	attrs := []any{"service", cfg.Service, "instance", instance}
	files := &Files{access: access, runtime: runtime, InstanceID: instance}
	files.Access = New(access, slog.LevelInfo).With(attrs...).With("log_type", "access")
	files.Runtime = New(runtime, level).With(attrs...).With("log_type", "runtime")
	files.Framework = newFrameworkLogger(runtime, level, append(attrs, "log_type", "runtime", "component", "hertz"))
	return files
}

func (f *Files) Close() error { return errors.Join(f.access.Close(), f.runtime.Close()) }

type logWriter struct {
	out      io.Writer
	rolling  *lumberjack.Logger // Only rolling files are owned by Files.
	mu       sync.Mutex
	reported bool
	closed   bool
	fallback io.Writer
}

func openWriter(path string, cfg Options) (*logWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	rolling := &lumberjack.Logger{Filename: path, MaxSize: cfg.MaxSizeMB, MaxBackups: cfg.MaxBackups, MaxAge: cfg.MaxAgeDays}
	return &logWriter{out: rolling, rolling: rolling, fallback: os.Stderr}, nil
}

var errStdoutWrite = errors.New("stdout logging failed")

func (w *logWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	n, err := w.out.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		w.reported = false
		return n, nil
	}
	if w.rolling == nil {
		err = errStdoutWrite
	}
	if !w.reported {
		if w.rolling == nil {
			fmt.Fprintln(w.fallback, err)
		} else {
			fmt.Fprintf(w.fallback, "file logging failed for %s: %v\n", filepath.Base(w.rolling.Filename), err)
		}
		w.reported = true
	}
	return n, err
}

func (w *logWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.rolling != nil {
		return w.rolling.Close()
	}
	return nil
}
