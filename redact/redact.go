// Package redact removes sensitive structured values before log serialization.
// Callers must still avoid raw request bodies, credentials and unsafe free text.
package redact

import (
	"bytes"
	"encoding/json"
	"strings"
)

const Mask = "***"

func Sensitive(key string) bool {
	key = strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(key))
	switch key {
	case "key", "authorization", "cookie", "setcookie", "credentials", "credential", "sign", "signature", "proof", "identityassertion", "passwordhash", "account", "accountnumber", "bankaccount", "bankcard", "certno", "phone", "email", "body", "requestbody", "responsebody", "payload":
		return true
	}
	for _, part := range []string{"password", "passwd", "secret", "privatekey", "token", "sharedkey", "apikey"} {
		if strings.Contains(key, part) {
			return true
		}
	}
	return false
}

// JSON returns a redacted copy and preserves JSON numbers without float64 conversion.
func JSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return json.Marshal(clean(decoded))
}

func clean(value any) any {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if Sensitive(key) {
				value[key] = Mask
			} else {
				value[key] = clean(child)
			}
		}
	case []any:
		for index, child := range value {
			value[index] = clean(child)
		}
	}
	return value
}
