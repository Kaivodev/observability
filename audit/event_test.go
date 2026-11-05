package audit

import (
	"context"
	"strings"
	"testing"

	"github.com/Kaivodev/observability/requestmeta"
	"github.com/google/uuid"
)

func TestPrepare(t *testing.T) {
	ids := requestmeta.New()
	ctx := requestmeta.With(context.Background(), ids)
	event := Event{ID: uuid.New(), ActorType: "system", ActorID: "test", Action: "entry.post", TargetType: "entry", TargetID: uuid.NewString(), Result: "succeeded", Metadata: map[string]any{"token": "private-value", "amount": "100"}}
	record, err := Prepare(ctx, "ledger", event)
	if err != nil {
		t.Fatal(err)
	}
	if record.Service != "ledger" || record.TraceID != ids.TraceID || record.RequestID != ids.RequestID || record.ID != event.ID {
		t.Fatal("identity/correlation lost")
	}
	if strings.Contains(string(record.Changes), "private-value") || !strings.Contains(string(record.Changes), "100") {
		t.Fatal("redaction failed")
	}
	if event.Metadata["token"] != "private-value" {
		t.Fatal("caller data mutated")
	}
	if _, err := Prepare(context.Background(), "ledger", event); err == nil {
		t.Fatal("missing context accepted")
	}
	for _, mutate := range []func(*Event){
		func(e *Event) { e.ID = uuid.Nil }, func(e *Event) { e.ActorType = "unknown" },
		func(e *Event) { e.Result = "maybe" }, func(e *Event) { e.BusinessID = "business-without-org" },
		func(e *Event) { e.Action = "unsafe\nvalue" }, func(e *Event) { e.Metadata = map[string]any{"text": strings.Repeat("x", 13<<10)} },
	} {
		invalid := event
		mutate(&invalid)
		if _, err := Prepare(ctx, "ledger", invalid); err == nil {
			t.Fatal("invalid event accepted")
		}
	}
}
