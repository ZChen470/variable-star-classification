package tests

import (
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClassificationCommandRoundTrip(t *testing.T) {
	want := &classificationv1.ClassificationCommand{
		JobId:                      "job-example",
		ObjectId:                   "object-001",
		CandidateRevision:          7,
		LightCurveRevision:         11,
		DeclaredEligibleEpochCount: 20,
		ModelBundleVersion:         "bundle-v1",
		ExecutionMode:              classificationv1.ExecutionMode_EXECUTION_MODE_PRODUCTION,
		Priority:                   classificationv1.ClassificationPriority_CLASSIFICATION_PRIORITY_REALTIME,
		CreatedAt: timestamppb.New(
			time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC),
		),
		TraceContext: &classificationv1.TraceContext{
			TraceId:       "trace-001",
			CorrelationId: "correlation-001",
			CausationId:   "candidate-event-001",
		},
	}

	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal ClassificationCommand: %v", err)
	}

	got := &classificationv1.ClassificationCommand{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("unmarshal ClassificationCommand: %v", err)
	}

	if !proto.Equal(want, got) {
		t.Fatalf("round-trip mismatch:\nwant: %v\ngot:  %v", want, got)
	}
}
