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
	Access    *slog.Logger
	Runtime   *slog.Logger
	Framework *frameworkLogger
	access    *fileWriter
	runtime   *fileWriter
}

// Open owns two rolling files. Use one directory per running process/instance.
func Open(cfg Options, level slog.Level) (*Files, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
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
	instance := cfg.InstanceID
	if instance == "" {
		instance = uuid.NewString()
	}
	attrs := []any{"service", cfg.Service, "instance", instance}
	files := &Files{access: access, runtime: runtime}
	files.Access = New(access, slog.LevelInfo).With(attrs...).With("log_type", "access")
	files.Runtime = New(runtime, level).With(attrs...).With("log_type", "runtime")
	files.Framework = newFrameworkLogger(runtime, level, append(attrs, "log_type", "runtime", "component", "hertz"))
	return files, nil
}

func (f *Files) Close() error { return errors.Join(f.access.Close(), f.runtime.Close()) }

type fileWriter struct {
	file     *lumberjack.Logger
	mu       sync.Mutex
	reported bool
	closed   bool
	fallback io.Writer
}

func openWriter(path string, cfg Options) (*fileWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return &fileWriter{file: &lumberjack.Logger{Filename: path, MaxSize: cfg.MaxSizeMB, MaxBackups: cfg.MaxBackups, MaxAge: cfg.MaxAgeDays}, fallback: os.Stderr}, nil
}

func (w *fileWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	n, err := w.file.Write(data)
	if err != nil {
		if !w.reported {
			fmt.Fprintf(w.fallback, "file logging failed for %s: %v\n", filepath.Base(w.file.Filename), err)
			w.reported = true
		}
	} else {
		w.reported = false
	}
	return n, err
}

func (w *fileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}
