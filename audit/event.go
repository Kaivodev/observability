// Package audit defines validated, redacted operation events. Storage, migrations
// and transactions remain owned by each service.
package audit

import (
	"context"
	"errors"
	"regexp"
	"unicode/utf8"

	"github.com/Kaivodev/observability/redact"
	"github.com/Kaivodev/observability/requestmeta"
	"github.com/google/uuid"
)

type Event struct {
	ID             uuid.UUID
	ActorType      string
	ActorID        string
	ActingRoleID   string
	OrganizationID string
	BusinessID     string
	Action         string
	TargetType     string
	TargetID       string
	Result         string
	ReasonCode     string
	Before         map[string]any
	After          map[string]any
	Metadata       map[string]any
}

// Record is the safe storage representation returned by Prepare. The service's
// adapter maps it to its own SQL parameters and writes in the business transaction.
type Record struct {
	ID             uuid.UUID
	Service        string
	ActorType      string
	ActorID        string
	ActingRoleID   string
	OrganizationID string
	BusinessID     string
	Action         string
	TargetType     string
	TargetID       string
	Result         string
	ReasonCode     string
	RequestID      string
	TraceID        string
	Changes        []byte
}

type Recorder interface {
	Append(context.Context, Event) error
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// Prepare validates event shape, correlation and size; it does not authenticate
// an actor or authorize an action. Actor fields must come from verified identity.
func Prepare(ctx context.Context, service string, event Event) (Record, error) {
	metadata := requestmeta.From(ctx)
	if !metadata.Valid() {
		return Record{}, errors.New("operation audit requires request_id and trace_id")
	}
	if event.ID == uuid.Nil {
		return Record{}, errors.New("operation audit event ID is required")
	}
	for _, item := range []struct {
		value    string
		max      int
		required bool
	}{
		{service, 64, true}, {event.ActorID, 128, true}, {event.ActingRoleID, 128, false},
		{event.OrganizationID, 128, false}, {event.BusinessID, 128, false}, {event.Action, 128, true},
		{event.TargetType, 64, true}, {event.TargetID, 256, true}, {event.ReasonCode, 64, false},
	} {
		if item.value == "" && !item.required {
			continue
		}
		if len(item.value) > item.max || !utf8.ValidString(item.value) || !identifier.MatchString(item.value) {
			return Record{}, errors.New("invalid operation audit identifier")
		}
	}
	if event.BusinessID != "" && event.OrganizationID == "" {
		return Record{}, errors.New("business audit scope requires an organization")
	}
	switch event.ActorType {
	case "user", "merchant_application", "service", "system":
	default:
		return Record{}, errors.New("invalid operation audit actor type")
	}
	switch event.Result {
	case "succeeded", "failed", "denied", "accepted":
	default:
		return Record{}, errors.New("invalid operation audit result")
	}
	changes, err := redact.JSON(map[string]any{"before": event.Before, "after": event.After, "metadata": event.Metadata})
	if err != nil {
		return Record{}, errors.New("operation audit details cannot be encoded")
	}
	if len(changes) > 12<<10 {
		return Record{}, errors.New("operation audit details exceed 12 KiB")
	}
	return Record{
		ID: event.ID, Service: service, ActorType: event.ActorType, ActorID: event.ActorID,
		ActingRoleID: event.ActingRoleID, OrganizationID: event.OrganizationID, BusinessID: event.BusinessID,
		Action: event.Action, TargetType: event.TargetType, TargetID: event.TargetID, Result: event.Result,
		ReasonCode: event.ReasonCode, RequestID: metadata.RequestID, TraceID: metadata.TraceID, Changes: changes,
	}, nil
}
