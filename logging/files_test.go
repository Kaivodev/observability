package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Kaivodev/observability/redact"
	"github.com/Kaivodev/observability/requestmeta"
	"go.opentelemetry.io/otel/trace"
)

func testLoggingConfig(t *testing.T) Options {
	return Options{Service: "ledger", Dir: t.TempDir(), InstanceID: "test-instance", MaxSizeMB: 1, MaxBackups: 2, MaxAgeDays: 1}
}

func TestOutputFieldsCorrelationAndRedaction(t *testing.T) {
	for _, mode := range []string{"file", "stdout"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testLoggingConfig(t)
			cfg.Output = mode
			cfg.Dir = filepath.Join(cfg.Dir, "logs")
			var stdout bytes.Buffer
			files, err := openWithOutput(cfg, slog.LevelError, &stdout, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			metadata := requestmeta.New()
			span := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
			})
			ctx := trace.ContextWithSpanContext(requestmeta.With(context.Background(), metadata), span)
			details := map[string]any{"nested": map[string]any{"access_token": "do-not-record-token"}}
			files.Access.InfoContext(ctx, "http request", "trace_id", "stale")
			files.Runtime.Info("filtered")
			files.Runtime.ErrorContext(ctx, "operation failed", "password", "do-not-record-password", "details", details,
				slog.Group("credentials", slog.String("value", "do-not-record-group")))
			files.Framework.CtxErrorf(ctx, "framework diagnostic")
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			data := stdout.Bytes()
			if mode == "file" {
				read := func(name string) []byte {
					b, err := os.ReadFile(filepath.Join(cfg.Dir, name+".log"))
					if err != nil {
						t.Fatal(err)
					}
					return b
				}
				access, runtime := read("access"), read("runtime")
				if len(data) != 0 || bytes.Count(access, []byte("\n")) != 1 || !bytes.Contains(access, []byte("http request")) {
					t.Fatal("access and runtime outputs mixed")
				}
				data = append(access, runtime...)
			} else if _, err := os.Stat(cfg.Dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stdout touched the log directory: %v", err)
			}
			if bytes.Contains(data, []byte("do-not-record")) || bytes.Contains(data, []byte("stale")) {
				t.Fatal("sensitive values or stale correlation leaked")
			}
			if details["nested"].(map[string]any)["access_token"] != "do-not-record-token" {
				t.Fatal("redaction modified caller data")
			}
			want := []struct{ msg, kind, level string }{
				{"http request", "access", "INFO"}, {"operation failed", "runtime", "ERROR"}, {"framework diagnostic", "runtime", "ERROR"},
			}
			lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
			if len(lines) != len(want) {
				t.Fatalf("got %d records", len(lines))
			}
			for i, line := range lines {
				var r map[string]any
				if err := json.Unmarshal(line, &r); err != nil {
					t.Fatal(err)
				}
				if r["msg"] != want[i].msg || r["log_type"] != want[i].kind || r["level"] != want[i].level ||
					r["service"] != cfg.Service || r["instance"] != cfg.InstanceID || r["request_id"] != metadata.RequestID ||
					r["trace_id"] != span.TraceID().String() || r["span_id"] != span.SpanID().String() || r["trace_sampled"] != true {
					t.Fatalf("unexpected log fields: %v", r)
				}
				if want[i].msg == "operation failed" && r["password"] != redact.Mask {
					t.Fatal("missing redaction marker")
				}
				if want[i].msg == "framework diagnostic" && r["component"] != "hertz" {
					t.Fatal("missing framework component")
				}
				if !strings.HasSuffix(r["time"].(string), "Z") {
					t.Fatal("timestamp is not UTC")
				}
			}
			for _, log := range []*slog.Logger{files.Access, files.Runtime} {
				if err := log.Handler().Handle(ctx, slog.NewRecord(time.Now(), slog.LevelError, "after close", 0)); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("write after close: %v", err)
				}
			}
		})
	}
}

func TestRotationRetentionAndConcurrency(t *testing.T) {
	cfg := testLoggingConfig(t)
	for i := 1; i <= 4; i++ {
		stamp := time.Now().UTC().Add(-time.Duration(i) * time.Hour).Format("2006-01-02T15-04-05.000")
		if err := os.WriteFile(filepath.Join(cfg.Dir, "runtime-"+stamp+".log"), []byte("old"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	files, err := Open(cfg, slog.LevelInfo)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	files.Runtime.Info(strings.Repeat("x", 600<<10))
	time.Sleep(2 * time.Millisecond)
	files.Runtime.Info(strings.Repeat("y", 600<<10))
	deadline := time.Now().Add(3 * time.Second)
	for {
		backups, err := filepath.Glob(filepath.Join(cfg.Dir, "runtime-*.log"))
		if err != nil {
			t.Fatal(err)
		}
		if len(backups) > 0 && len(backups) <= cfg.MaxBackups {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotation cleanup did not converge: %v", backups)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var group sync.WaitGroup
	for i := range 50 {
		group.Go(func() { files.Access.Info("concurrent request", "index", i) })
	}
	group.Wait()
	data, err := os.ReadFile(filepath.Join(cfg.Dir, "access.log"))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) != 50 {
		t.Fatalf("got %d records", len(lines))
	}
	for _, line := range lines {
		if !json.Valid(line) {
			t.Fatal("concurrent records interleaved")
		}
	}
}

func TestFileErrorsAreVisibleWithoutEchoingPayload(t *testing.T) {
	cfg := testLoggingConfig(t)
	writer, err := openWriter(filepath.Join(cfg.Dir, "runtime.log"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var fallback bytes.Buffer
	writer.fallback = &fallback
	tooLarge := bytes.Repeat([]byte("secret-payload"), 100000)
	for range 2 {
		if _, err := writer.Write(tooLarge); err == nil {
			t.Fatal("expected oversized write to fail")
		}
	}
	if strings.Count(fallback.String(), "file logging failed") != 1 || strings.Contains(fallback.String(), "secret-payload") {
		t.Fatalf("unexpected fallback: %s", fallback.String())
	}
	if _, err := writer.Write([]byte("ok\n")); err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write(tooLarge)
	if strings.Count(fallback.String(), "file logging failed") != 2 {
		t.Fatal("a new failure after recovery must be reported")
	}
	blocked := filepath.Join(cfg.Dir, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Dir = blocked
	if _, err := Open(cfg, slog.LevelInfo); err == nil {
		t.Fatal("invalid log directory must fail startup")
	}
}

// Each call captures exactly one underlying Write. Deliberately unsynchronized:
// all three loggers must be serialized by the shared stdout writer.
type stdoutCapture struct {
	writes []string
	fail   bool
	closed bool
}

func (w *stdoutCapture) Write(p []byte) (int, error) {
	if w.fail {
		return 0, errors.New("sensitive writer detail")
	}
	w.writes = append(w.writes, string(p))
	return len(p), nil
}
func (w *stdoutCapture) Close() error { w.closed = true; return nil }

func TestSharedStdoutConcurrencyAndFailureEpisodes(t *testing.T) {
	cfg := testLoggingConfig(t)
	cfg.Output, cfg.Dir = "stdout", ""
	var out stdoutCapture
	var stderr bytes.Buffer
	logs, err := openWithOutput(cfg, slog.LevelInfo, &out, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := range 100 {
		group.Go(func() { logs.Access.Info("access", "index", i) })
		group.Go(func() { logs.Runtime.Info("runtime", "index", i) })
		group.Go(func() { logs.Framework.Infof("framework %d", i) })
	}
	group.Wait()
	if len(out.writes) != 300 {
		t.Fatalf("got %d writes for 300 records", len(out.writes))
	}
	for _, data := range out.writes {
		if strings.Count(data, "\n") != 1 || !strings.HasSuffix(data, "\n") || !json.Valid([]byte(data)) {
			t.Fatal("underlying write did not contain exactly one JSON line")
		}
	}
	out.fail = true
	logs.Access.Info("sensitive record")
	logs.Framework.Errorf("sensitive record")
	out.fail = false
	logs.Runtime.Info("recovered")
	out.fail = true
	logs.Runtime.Error("sensitive record")
	if got := stderr.String(); got != "stdout logging failed\nstdout logging failed\n" {
		t.Fatalf("unexpected stderr: %q", got)
	}
	if err := logs.Close(); err != nil || out.closed {
		t.Fatalf("Close must leave stdout open: closed=%v, err=%v", out.closed, err)
	}
}
