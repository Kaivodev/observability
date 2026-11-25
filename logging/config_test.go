package logging

import "testing"

func TestLoadConfig(t *testing.T) {
	for _, service := range []string{"backend", "iam", "ledger", "settlement", "reconciliation"} {
		o, _, err := Load(service, func(string) string { return "" })
		if err != nil || o.Service != service || o.Dir != "./logs" || o.MaxSizeMB != 50 || o.MaxBackups != 5 || o.MaxAgeDays != 14 {
			t.Fatalf("defaults: %+v, %v", o, err)
		}
	}
	for _, tc := range []struct{ key, value string }{
		{"LOG_DIR", " "}, {"LOG_MAX_SIZE_MB", "0"}, {"LOG_MAX_BACKUPS", "1001"},
		{"LOG_MAX_AGE_DAYS", "never"}, {"LOG_LEVEL", "verbose"}, {"INSTANCE_ID", "bad\nname"},
	} {
		_, _, err := Load("iam", func(key string) string {
			if key == "AIPAY_"+tc.key {
				return tc.value
			}
			return ""
		})
		if err == nil {
			t.Errorf("accepted invalid %s", tc.key)
		}
	}
	if _, _, err := Load("iam", nil); err == nil {
		t.Fatal("accepted missing env reader")
	}
	if _, _, err := Load("", func(string) string { return "" }); err == nil {
		t.Fatal("accepted missing service")
	}
}

func TestOutputSelection(t *testing.T) {
	for _, test := range []struct{ host, output, want string }{
		{"", "", "file"}, {"kube", "", "stdout"},
		{"kube", "file", "file"}, {"", "stdout", "stdout"},
		{"kube", "STDOUT", ""}, {"", " ", ""},
	} {
		values := map[string]string{"AIPAY_LOG_OUTPUT": test.output, "KUBERNETES_SERVICE_HOST": test.host}
		cfg, _, err := Load("ledger", func(k string) string { return values[k] })
		if test.want == "" {
			if err == nil {
				t.Error("accepted invalid output", test.output)
			}
		} else if err != nil || cfg.Output != test.want {
			t.Errorf("host %q, output %q: got %q, %v", test.host, test.output, cfg.Output, err)
		}
	}
	values := map[string]string{"AIPAY_LOG_OUTPUT": "stdout", "AIPAY_LOG_DIR": " "}
	get := func(k string) string { return values[k] }
	if _, _, err := Load("ledger", get); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"AIPAY_LOG_MAX_SIZE_MB", "AIPAY_LOG_MAX_BACKUPS", "AIPAY_LOG_MAX_AGE_DAYS"} {
		for _, v := range []string{"invalid", "0"} {
			values[key] = v
			if _, _, err := Load("ledger", get); err == nil {
				t.Errorf("stdout accepted invalid %s", key)
			}
		}
		delete(values, key)
	}
}
