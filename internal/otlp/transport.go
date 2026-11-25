// Package otlp shares the transport policy and resource identity of both signals.
package otlp

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Transport struct {
	Endpoint                  string
	AllowInsecure             bool
	CAFile, CertFile, KeyFile string
}

func (c Transport) Validate(signal string) error {
	if (c.CertFile == "") != (c.KeyFile == "") {
		return fmt.Errorf("%s client certificate and key must be configured together", signal)
	}
	if c.Endpoint == "" {
		if c.CAFile != "" || c.CertFile != "" {
			return fmt.Errorf("%s TLS files require an export endpoint", signal)
		}
		return nil
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("%s endpoint must be an http(s) authority without credentials, path or query", signal)
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("invalid %s endpoint port", signal)
		}
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) && !c.AllowInsecure {
			return fmt.Errorf("non-loopback plaintext %s export requires AIPAY_%s_ALLOW_INSECURE=true", signal, strings.ToUpper(signal))
		}
		if c.CAFile != "" || c.CertFile != "" {
			return fmt.Errorf("%s TLS files require an https endpoint", signal)
		}
	}
	return nil
}

// TLSConfig returns nil for a validated plaintext endpoint.
func (c Transport) TLSConfig(signal string) (*tls.Config, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid %s endpoint", signal)
	}
	if u.Scheme == "http" {
		return nil, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: u.Hostname()}
	if c.CAFile != "" {
		data, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read %s CA bundle failed", signal)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("%s CA bundle contains no certificates", signal)
		}
		tlsConfig.RootCAs = roots
	}
	if c.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load %s client certificate failed", signal)
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	return tlsConfig, nil
}

func ValidResourceValue(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < ' ' || r == 127 {
			return false
		}
	}
	return true
}

func Resource(service, version, instance, environment string) *resource.Resource {
	return resource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", version),
		attribute.String("service.instance.id", instance),
		attribute.String("deployment.environment.name", environment),
	)
}
