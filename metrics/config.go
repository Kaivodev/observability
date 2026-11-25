// Package metrics provides explicit SDK meter providers and bounded OTLP/gRPC export.
package metrics

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Kaivodev/observability/internal/otlp"
)

type Config struct {
	Endpoint         string
	AllowInsecure    bool
	CAFile           string
	CertFile         string
	KeyFile          string
	Interval         time.Duration
	ExportTimeout    time.Duration
	ShutdownTimeout  time.Duration
	Environment      string
	CardinalityLimit int
}

// Load reads AIPAY_METRICS_* settings. An empty endpoint disables export.
func Load(get func(string) string) (Config, error) {
	if get == nil {
		return Config{}, errors.New("metrics requires an environment reader")
	}
	value := func(key, fallback string) string {
		if v := get("AIPAY_METRICS_" + key); v != "" {
			return v
		}
		return fallback
	}
	var problems []error
	duration := func(key, fallback string) time.Duration {
		d, err := time.ParseDuration(value(key, fallback))
		if err != nil {
			problems = append(problems, fmt.Errorf("AIPAY_METRICS_%s must be a duration", key))
		}
		return d
	}
	allowInsecure, err := strconv.ParseBool(value("ALLOW_INSECURE", "false"))
	if err != nil {
		problems = append(problems, errors.New("AIPAY_METRICS_ALLOW_INSECURE must be a boolean"))
	}
	limit, err := strconv.Atoi(value("CARDINALITY_LIMIT", "2000"))
	if err != nil {
		problems = append(problems, errors.New("AIPAY_METRICS_CARDINALITY_LIMIT must be an integer"))
	}
	c := Config{
		Endpoint: value("ENDPOINT", ""), AllowInsecure: allowInsecure,
		CAFile: value("CA_FILE", ""), CertFile: value("CERT_FILE", ""), KeyFile: value("KEY_FILE", ""),
		Interval: duration("INTERVAL", "15s"), ExportTimeout: duration("EXPORT_TIMEOUT", "5s"),
		ShutdownTimeout: duration("SHUTDOWN_TIMEOUT", "3s"), Environment: value("ENVIRONMENT", "development"),
		CardinalityLimit: limit,
	}
	return c, errors.Join(append(problems, c.Validate())...)
}

func (c Config) Validate() error {
	switch {
	case c.Interval < time.Second || c.Interval > 5*time.Minute:
		return errors.New("metrics interval must be between 1s and 5m")
	case c.ExportTimeout <= 0 || c.ExportTimeout >= c.Interval:
		return errors.New("metrics export timeout must be positive and less than the interval")
	case c.ShutdownTimeout <= 0:
		return errors.New("metrics shutdown timeout must be positive")
	case c.CardinalityLimit < 100 || c.CardinalityLimit > 100000:
		return errors.New("metrics cardinality limit must be between 100 and 100000")
	case c.Environment == "" || len(c.Environment) > 64 || strings.ContainsAny(c.Environment, "\r\n\x00"):
		return errors.New("invalid metrics deployment environment")
	}
	return c.transport().Validate("metrics")
}

func (c Config) transport() otlp.Transport {
	return otlp.Transport{Endpoint: c.Endpoint, AllowInsecure: c.AllowInsecure, CAFile: c.CAFile, CertFile: c.CertFile, KeyFile: c.KeyFile}
}
