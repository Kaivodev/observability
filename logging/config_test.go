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
