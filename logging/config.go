package logging

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
)

// Options describes one process's two log files. Service is supplied by the
// composition root, never by request data. Each instance needs its own directory.
type Options struct {
	Service    string
	Dir        string
	InstanceID string
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
}

var serviceName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func (o Options) Validate() error {
	var problems []error
	if !serviceName.MatchString(o.Service) {
		problems = append(problems, errors.New("invalid log service name"))
	}
	if strings.TrimSpace(o.Dir) == "" {
		problems = append(problems, errors.New("AIPAY_LOG_DIR cannot be blank"))
	}
	if len(o.InstanceID) > 128 || strings.ContainsAny(o.InstanceID, "\r\n\x00") {
		problems = append(problems, errors.New("AIPAY_INSTANCE_ID is invalid"))
	}
	for _, item := range []struct {
		name       string
		value, max int
	}{
		{"LOG_MAX_SIZE_MB", o.MaxSizeMB, 1024},
		{"LOG_MAX_BACKUPS", o.MaxBackups, 1000},
		{"LOG_MAX_AGE_DAYS", o.MaxAgeDays, 3650},
	} {
		if item.value < 1 || item.value > item.max {
			problems = append(problems, fmt.Errorf("AIPAY_%s must be between 1 and %d", item.name, item.max))
		}
	}
	return errors.Join(problems...)
}

// Load shares defaults and validation across services without mutating process
// environment or initializing loggers. Pass os.Getenv in a process entry point.
func Load(service string, get func(string) string) (Options, slog.Level, error) {
	if get == nil {
		return Options{}, 0, errors.New("log configuration requires an environment reader")
	}
	value := func(key, fallback string) string {
		if v := get("AIPAY_" + key); v != "" {
			return v
		}
		return fallback
	}
	var problems []error
	integer := func(key, fallback string) int {
		v, err := strconv.Atoi(value(key, fallback))
		if err != nil {
			problems = append(problems, fmt.Errorf("AIPAY_%s must be an integer", key))
		}
		return v
	}
	o := Options{
		Service: service, Dir: value("LOG_DIR", "./logs"), InstanceID: get("AIPAY_INSTANCE_ID"),
		MaxSizeMB: integer("LOG_MAX_SIZE_MB", "50"), MaxBackups: integer("LOG_MAX_BACKUPS", "5"),
		MaxAgeDays: integer("LOG_MAX_AGE_DAYS", "14"),
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(value("LOG_LEVEL", "info"))); err != nil {
		problems = append(problems, errors.New("AIPAY_LOG_LEVEL must be debug, info, warn or error"))
	}
	return o, level, errors.Join(append(problems, o.Validate())...)
}
