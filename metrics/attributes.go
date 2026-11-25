package metrics

import "strings"

var separators = strings.NewReplacer(".", "_", "-", "_")

// Forbidden reports whether the provider removes this measurement attribute.
func Forbidden(key string) bool {
	key = separators.Replace(strings.ToLower(key))
	switch key {
	case "account_id", "user_id", "org_id", "organization_id", "business_id", "application_id", "merchant_id",
		"order_id", "trade_no", "out_trade_no", "upstream_trade_no", "notification_id", "idempotency_key",
		"request_id", "trace_id", "span_id", "task_id", "chat_id", "email", "phone", "address", "tx_hash",
		"ip", "url", "query", "error", "error_message":
		return true
	}
	for _, suffix := range [...]string{"_id", "_ip", "_address", "_email", "_phone", "_hash", "_url"} {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	for _, prefix := range [...]string{"net_", "network_", "server_address", "server_port", "client_", "url_", "http_url", "http_target", "user_", "enduser_"} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
