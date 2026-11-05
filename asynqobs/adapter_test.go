package asynqobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/Kaivodev/observability/logging"
	"github.com/Kaivodev/observability/requestmeta"
	"github.com/hibiken/asynq"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type fakeQueue struct {
	task *asynq.Task
	err  error
}

func (q *fakeQueue) EnqueueContext(ctx context.Context, task *asynq.Task, options ...asynq.Option) (*asynq.TaskInfo, error) {
	q.task = task
	if q.err != nil {
		return nil, q.err
	}
	return &asynq.TaskInfo{ID: "task-one", Queue: "iam", Type: task.Type()}, nil
}

func TestProducerAndEachWorkerExecutionAreCorrelated(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	var access, runtime bytes.Buffer
	queue := &fakeQueue{}
	producer, err := NewProducer(queue, provider, logging.New(&access, slog.LevelInfo), 1024)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(provider, logging.New(&access, slog.LevelInfo), logging.New(&runtime, slog.LevelInfo), 1024)
	if err != nil {
		t.Fatal(err)
	}
	origin, span := provider.Tracer("test").Start(context.Background(), "origin")
	origin = requestmeta.Ensure(origin)
	message := NewMessage(origin, "iam.email.v1", []byte("private-task-body"))
	// Serialize the carrier/message exactly as a service-owned outbox can do.
	encoded, _ := json.Marshal(message)
	var stored Message
	if err = json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	span.End()
	if _, err = producer.Enqueue(context.Background(), stored, asynq.TaskID("task-one")); err != nil {
		t.Fatal(err)
	}
	originalTrace := span.SpanContext().TraceID()
	attempts := []trace.SpanContext{}
	handler := worker.Middleware(asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		observed := trace.SpanContextFromContext(ctx)
		attempts = append(attempts, observed)
		if requestmeta.From(ctx).TraceID != observed.TraceID().String() || requestmeta.From(ctx).RequestID != requestmeta.From(origin).RequestID {
			t.Fatal("worker log context drift")
		}
		if string(task.Payload()) != "private-task-body" {
			t.Fatal("payload modified")
		}
		return nil
	}))
	for i := 0; i < 2; i++ {
		if err = handler.ProcessTask(context.Background(), queue.task); err != nil {
			t.Fatal(err)
		}
	}
	if attempts[0].TraceID() != originalTrace || attempts[1].TraceID() != originalTrace || attempts[0].SpanID() == attempts[1].SpanID() {
		t.Fatal("retries did not keep the trace and create independent spans")
	}
	spans := recorder.Ended()
	if len(spans) != 4 {
		t.Fatalf("got %d spans", len(spans))
	}
	producerSpan := spans[1]
	if producerSpan.Parent().SpanID() != span.SpanContext().SpanID() || spans[2].Parent().SpanID() != producerSpan.SpanContext().SpanID() {
		t.Fatal("producer/consumer parent relationships were lost")
	}
	if strings.Contains(access.String(), "private-task-body") || strings.Contains(runtime.String(), "private-task-body") {
		t.Fatal("payload leaked into logs")
	}
	// A long-delay/replayed message creates a new trace with an explicit link.
	stored.NewTrace = true
	if _, err = producer.Enqueue(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err = handler.ProcessTask(context.Background(), queue.task); err != nil {
		t.Fatal(err)
	}
	last := recorder.Ended()[5]
	if last.SpanContext().TraceID() == originalTrace || len(last.Links()) != 1 || last.Links()[0].SpanContext.TraceID() != originalTrace {
		t.Fatal("linked replay did not create a new trace linked to its source")
	}
}

func TestWorkerFailureSafetyRetrySignalsAndErrorHandlerContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	private := errors.New("never-log-this-password")
	for _, tc := range []struct {
		name    string
		failure error
		panic   bool
	}{
		{"failure", private, false}, {"skip", errors.Join(asynq.SkipRetry, private), false}, {"revoke", errors.Join(asynq.RevokeTask, private), false}, {"panic", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var access, runtime bytes.Buffer
			worker, err := NewWorker(provider, logging.New(&access, slog.LevelInfo), logging.New(&runtime, slog.LevelInfo), 1024)
			if err != nil {
				t.Fatal(err)
			}
			handler := worker.Middleware(asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
				if tc.panic {
					panic("never-log-this-panic")
				}
				return tc.failure
			}))
			err = handler.ProcessTask(context.Background(), asynq.NewTask("iam.email.v1", []byte("never-log-this-payload")))
			if err == nil || strings.Contains(err.Error(), "never-log-this") {
				t.Fatal("raw task error escaped middleware")
			}
			if tc.failure != nil && !errors.Is(err, tc.failure) {
				t.Fatal("retry-control error chain lost")
			}
			worker.HandleError(context.Background(), asynq.NewTask("iam.email.v1", nil), err)
			if strings.Contains(access.String(), "never-log-this") || strings.Contains(runtime.String(), "never-log-this") {
				t.Fatal("task data leaked into logs")
			}
			var accessRecord, runtimeRecord map[string]any
			if err = json.Unmarshal(bytes.TrimSpace(access.Bytes()), &accessRecord); err != nil {
				t.Fatal(err)
			}
			lines := bytes.Split(bytes.TrimSpace(runtime.Bytes()), []byte("\n"))
			if err = json.Unmarshal(lines[len(lines)-1], &runtimeRecord); err != nil {
				t.Fatal(err)
			}
			if accessRecord["trace_id"] != runtimeRecord["trace_id"] || accessRecord["span_id"] != runtimeRecord["span_id"] {
				t.Fatal("ErrorHandler lost the attempt context")
			}
		})
	}
}

func TestInvalidMetadataNeverInvokesBusinessHandler(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	var output bytes.Buffer
	log := logging.New(&output, slog.LevelInfo)
	worker, _ := NewWorker(provider, log, log, 32)
	called := false
	handler := worker.Middleware(asynq.HandlerFunc(func(context.Context, *asynq.Task) error { called = true; return nil }))
	task := asynq.NewTaskWithHeaders("iam.email.v1", nil, map[string]string{"traceparent": "never-log-this"})
	err := handler.ProcessTask(context.Background(), task)
	if called || !errors.Is(err, asynq.SkipRetry) || strings.Contains(output.String(), "never-log-this") {
		t.Fatal("invalid metadata accepted, retried, or exposed")
	}
	NewFrameworkLogger(log).Error("secret redis://credential:never-log-this@host")
	if strings.Contains(output.String(), "never-log-this") {
		t.Fatal("framework logger leaked free-form input")
	}
}
