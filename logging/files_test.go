package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Kaivodev/observability/requestmeta"
)

func testLoggingConfig(t *testing.T) Options {
	return Options{Service: "ledger", Dir: t.TempDir(), InstanceID: "test-instance", MaxSizeMB: 1, MaxBackups: 2, MaxAgeDays: 1}
}

func TestFileSeparationCorrelationAndRedaction(t *testing.T) {
	cfg := testLoggingConfig(t)
	files, err := Open(cfg, slog.LevelError)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	metadata := requestmeta.New()
	ctx := requestmeta.With(context.Background(), metadata)
	details := map[string]any{"safe": "kept", "nested": map[string]any{"access_token": "do-not-record-token"}}
	files.Access.InfoContext(ctx, "http request", "route", "/api.php", "status", 200)
	files.Runtime.Info("filtered")
	files.Runtime.ErrorContext(ctx, "operation failed", "password", "do-not-record-password", "details", details, slog.Group("credentials", slog.String("value", "do-not-record-group")))
	files.Framework.CtxErrorf(ctx, "framework diagnostic")
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	access, err := os.ReadFile(filepath.Join(cfg.Dir, "access.log"))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := os.ReadFile(filepath.Join(cfg.Dir, "runtime.log"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(runtime, []byte("http request")) || bytes.Contains(access, []byte("operation failed")) || bytes.Contains(runtime, []byte("filtered")) {
		t.Fatal("logs mixed or runtime threshold ignored")
	}
	if bytes.Contains(runtime, []byte("do-not-record")) {
		t.Fatalf("sensitive structured value leaked: %s", runtime)
	}
	if details["nested"].(map[string]any)["access_token"] != "do-not-record-token" {
		t.Fatal("redaction modified caller data")
	}
	for _, data := range [][]byte{access, runtime} {
		for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
			var record map[string]any
			if err := json.Unmarshal(line, &record); err != nil {
				t.Fatal(err)
			}
			if record["request_id"] != metadata.RequestID || record["trace_id"] != metadata.TraceID || record["service"] != "ledger" || record["instance"] != "test-instance" {
				t.Fatalf("missing correlation/source: %v", record)
			}
			if _, ok := record["level"].(string); !ok {
				t.Fatalf("level is not a string: %v", record)
			}
			if !strings.HasSuffix(record["time"].(string), "Z") {
				t.Fatal("log time must be UTC")
			}
		}
	}
	if _, err := files.runtime.Write([]byte("after close")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed logger reopened: %v", err)
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
