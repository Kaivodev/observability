package metrics

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func loadValues(values map[string]string) (Config, error) {
	return Load(func(key string) string { return values[strings.TrimPrefix(key, "AIPAY_METRICS_")] })
}

func TestLoadConfiguration(t *testing.T) {
	cfg, err := Load(func(key string) string {
		if !strings.HasPrefix(key, "AIPAY_METRICS_") {
			t.Errorf("unexpected key %q", key)
		}
		return ""
	})
	want := Config{Interval: 15 * time.Second, ExportTimeout: 5 * time.Second, ShutdownTimeout: 3 * time.Second,
		CardinalityLimit: 2000, Environment: "development"}
	if err != nil || cfg != want {
		t.Fatalf("defaults: %+v, %v", cfg, err)
	}
	if _, err := Load(nil); err == nil {
		t.Fatal("accepted nil reader")
	}
	for _, v := range []map[string]string{
		{"ENDPOINT": "http://collector:4317", "ALLOW_INSECURE": "true"},
		{"ENDPOINT": "http://[::1]:4317"}, {"ENDPOINT": "https://collector:443", "CA_FILE": "ca", "CERT_FILE": "cert", "KEY_FILE": "key"},
		{"INTERVAL": "1s", "EXPORT_TIMEOUT": "999ms", "CARDINALITY_LIMIT": "100"},
		{"INTERVAL": "5m", "CARDINALITY_LIMIT": "100000", "ENVIRONMENT": "production"},
	} {
		if _, err := loadValues(v); err != nil {
			t.Errorf("valid config %v: %v", v, err)
		}
	}
	for _, v := range []map[string]string{
		{"ENDPOINT": "http://collector:4317"}, {"ENDPOINT": "collector:4317"}, {"ENDPOINT": "ftp://localhost"},
		{"ENDPOINT": "https://name:private-canary@collector"}, {"ENDPOINT": "https://collector/private-canary"},
		{"ENDPOINT": "https://collector?token=private-canary"}, {"ENDPOINT": "https://collector#private-canary"},
		{"ENDPOINT": "http://:4317"}, {"ENDPOINT": "http://localhost:0"}, {"ENDPOINT": "http://localhost:65536"},
		{"CA_FILE": "ca"}, {"CERT_FILE": "cert"}, {"KEY_FILE": "key"}, {"ENDPOINT": "http://localhost", "CA_FILE": "ca"},
		{"ALLOW_INSECURE": "private-canary"}, {"INTERVAL": "private-canary"}, {"INTERVAL": "999ms"}, {"INTERVAL": "301s"},
		{"EXPORT_TIMEOUT": "private-canary"}, {"EXPORT_TIMEOUT": "0s"}, {"EXPORT_TIMEOUT": "15s"}, {"EXPORT_TIMEOUT": "16s"},
		{"SHUTDOWN_TIMEOUT": "private-canary"}, {"SHUTDOWN_TIMEOUT": "0s"},
		{"CARDINALITY_LIMIT": "private-canary"}, {"CARDINALITY_LIMIT": "99"}, {"CARDINALITY_LIMIT": "100001"},
		{"ENVIRONMENT": "line\nbreak"}, {"ENVIRONMENT": strings.Repeat("a", 65)},
	} {
		if _, err := loadValues(v); err == nil || strings.Contains(err.Error(), "private-canary") {
			t.Errorf("invalid configuration accepted or exposed: %v", v)
		}
	}
}

func TestOpenValidation(t *testing.T) {
	cfg, _ := loadValues(nil)
	o := Options{Config: cfg, Service: "ledger", Instance: "pod"}
	log := slog.New(slog.DiscardHandler)
	check := func(o Options, log *slog.Logger) {
		t.Helper()
		if _, err := Open(context.Background(), o, log); err == nil || strings.Contains(err.Error(), "private-canary") {
			t.Fatal("invalid runtime options accepted or exposed")
		}
	}
	check(o, nil)
	for _, change := range []func(*Options){
		func(o *Options) { o.Service = "" }, func(o *Options) { o.Instance = "\x00" },
		func(o *Options) { o.Config.Environment = "" }, func(o *Options) { o.Config.ExportTimeout = -1 },
	} {
		invalid := o
		change(&invalid)
		check(invalid, log)
	}
	o.Config.Endpoint = "https://collector:4317"
	path := filepath.Join(t.TempDir(), "private-canary")
	o.Config.CAFile = path
	check(o, log)
	if err := os.WriteFile(path, []byte("private-canary"), 0600); err != nil {
		t.Fatal(err)
	}
	check(o, log)
	o.Config.CAFile = ""
	o.Config.CertFile, o.Config.KeyFile = path, path
	check(o, log)
}

func TestForbiddenAndDesignLabels(t *testing.T) {
	for _, key := range strings.Fields(`account_id user_id org_id organization_id business_id application_id merchant_id
		order_id trade_no out_trade_no upstream_trade_no notification_id idempotency_key request_id trace_id span_id
		task_id chat_id email phone address tx_hash ip url query error error_message peer.address custom.id peer-IP
		payer.Email payer-phone custom.Hash redirect-url NET.peer network.protocol server.AddressType server-port
		client.type Url.path http.URL.original http-target User.Agent enduser.role OUT-trade.No`) {
		if !Forbidden(key) {
			t.Errorf("did not block %q", key)
		}
	}
	for _, key := range strings.Fields(`dependency rpc_service rpc_method grpc_code peer_service reason listener
		http_route http_method http_status_code pool state queue task_type result stream outbox sampler status kind
		job instance deployment_environment_name kaivo_domain`) {
		if Forbidden(key) {
			t.Errorf("blocked design label %q", key)
		}
	}
}
