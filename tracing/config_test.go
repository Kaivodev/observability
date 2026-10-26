package tracing

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestConfigurationValidation(t *testing.T) {
	for _, values := range []map[string]string{
		{"ENDPOINT": "http://never-log-this:password@localhost:4317"},
		{"ENDPOINT": "https://collector:4317/private?token=never-log-this"},
		{"ENDPOINT": "http://collector:4317"},
		{"ENDPOINT": "http://127.0.0.1:65536"}, {"ENDPOINT": "http://127.0.0.1:0"},
		{"SAMPLE_RATIO": "NaN"}, {"SAMPLE_RATIO": "2"}, {"SAMPLE_RATIO": "invalid"},
		{"QUEUE_SIZE": "0"}, {"QUEUE_SIZE": "10", "BATCH_SIZE": "11"},
		{"CERT_FILE": "cert-only"}, {"EXPORT_TIMEOUT": "0s"}, {"SHUTDOWN_TIMEOUT": "-1s"},
	} {
		_, err := Load(func(key string) string { return values[strings.TrimPrefix(key, "AIPAY_TRACE_")] })
		if err == nil || strings.Contains(err.Error(), "never-log-this") {
			t.Fatalf("validation failed: %v", values)
		}
	}
	config, err := Load(func(key string) string {
		if key == "AIPAY_TRACE_ENDPOINT" {
			return "http://collector:4317"
		}
		if key == "AIPAY_TRACE_ALLOW_INSECURE" {
			return "true"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Endpoint = "https://collector:4317"
	config.CAFile = "/nonexistent/never-log-this"
	_, err = Open(context.Background(), Options{Config: config, Service: "iam", Instance: "test"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || strings.Contains(err.Error(), "never-log-this") {
		t.Fatal("missing CA file accepted or exposed")
	}
}
