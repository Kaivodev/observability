// Package tracing supplies OpenTelemetry configuration and safe propagation.
// It never owns business data or treats trace metadata as authorization.
package tracing

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Endpoint        string
	AllowInsecure   bool
	CAFile          string
	CertFile        string
	KeyFile         string
	SampleRatio     float64
	QueueSize       int
	BatchSize       int
	BatchInterval   time.Duration
	ExportTimeout   time.Duration
	ShutdownTimeout time.Duration
	Environment     string
}

// An empty endpoint keeps valid trace contexts and propagation without export.
func Load(get func(string) string) (Config, error) {
	if get == nil {
		return Config{}, errors.New("tracing requires an environment reader")
	}
	value := func(key, fallback string) string {
		if v := get("AIPAY_TRACE_" + key); v != "" {
			return v
		}
		return fallback
	}
	var problems []error
	number := func(key, fallback string) int {
		n, err := strconv.Atoi(value(key, fallback))
		if err != nil {
			problems = append(problems, fmt.Errorf("AIPAY_TRACE_%s must be an integer", key))
		}
		return n
	}
	duration := func(key, fallback string) time.Duration {
		d, err := time.ParseDuration(value(key, fallback))
		if err != nil {
			problems = append(problems, fmt.Errorf("AIPAY_TRACE_%s must be a duration", key))
		}
		return d
	}
	ratio, err := strconv.ParseFloat(value("SAMPLE_RATIO", "1"), 64)
	if err != nil {
		problems = append(problems, errors.New("AIPAY_TRACE_SAMPLE_RATIO must be a number"))
	}
	insecure, err := strconv.ParseBool(value("ALLOW_INSECURE", "false"))
	if err != nil {
		problems = append(problems, errors.New("AIPAY_TRACE_ALLOW_INSECURE must be a boolean"))
	}
	c := Config{Endpoint: get("AIPAY_TRACE_ENDPOINT"), AllowInsecure: insecure, CAFile: get("AIPAY_TRACE_CA_FILE"), CertFile: get("AIPAY_TRACE_CERT_FILE"), KeyFile: get("AIPAY_TRACE_KEY_FILE"), SampleRatio: ratio, QueueSize: number("QUEUE_SIZE", "2048"), BatchSize: number("BATCH_SIZE", "512"), BatchInterval: duration("BATCH_INTERVAL", "1s"), ExportTimeout: duration("EXPORT_TIMEOUT", "3s"), ShutdownTimeout: duration("SHUTDOWN_TIMEOUT", "3s"), Environment: value("ENVIRONMENT", "development")}
	return c, errors.Join(append(problems, c.Validate())...)
}

func (c Config) Validate() error {
	if math.IsNaN(c.SampleRatio) || math.IsInf(c.SampleRatio, 0) || c.SampleRatio < 0 || c.SampleRatio > 1 {
		return errors.New("trace sample ratio must be between 0 and 1")
	}
	if c.QueueSize < 1 || c.QueueSize > 65536 || c.BatchSize < 1 || c.BatchSize > c.QueueSize || c.BatchInterval <= 0 || c.ExportTimeout <= 0 || c.ShutdownTimeout <= 0 {
		return errors.New("invalid trace buffer or timeout configuration")
	}
	if c.Environment == "" || len(c.Environment) > 64 || strings.ContainsAny(c.Environment, "\r\n\x00") {
		return errors.New("invalid trace deployment environment")
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errors.New("trace client certificate and key must be configured together")
	}
	if c.Endpoint == "" {
		if c.CAFile != "" || c.CertFile != "" {
			return errors.New("trace TLS files require an export endpoint")
		}
		return nil
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("trace endpoint must be an http(s) authority without credentials, path or query")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("invalid trace endpoint port")
		}
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) && !c.AllowInsecure {
			return errors.New("non-loopback plaintext trace export requires AIPAY_TRACE_ALLOW_INSECURE=true")
		}
		if c.CAFile != "" || c.CertFile != "" {
			return errors.New("trace TLS files require an https endpoint")
		}
	}
	return nil
}

func DefaultConfig() Config            { c, _ := Load(func(string) string { return "" }); return c }
func LoadEnvironment() (Config, error) { return Load(os.Getenv) }
