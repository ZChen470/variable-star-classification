package main

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
)

const testMockCandidateTopic = "variable-star-candidate"

func TestCandidateEventPublisherMatchesCandidateDecoderContract(t *testing.T) {
	t.Parallel()

	dataset := candidatePublisherTestDataset()
	occurredAt := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)
	sink := &recordingMessagePublisher{}

	publisher, err := newCandidateEventPublisher(
		dataset,
		testMockCandidateTopic,
		2,
		occurredAt,
		sink,
	)
	if err != nil {
		t.Fatalf("newCandidateEventPublisher() error = %v", err)
	}

	if err := publisher.publishNext(context.Background()); err != nil {
		t.Fatalf("publishNext() error = %v", err)
	}

	if len(sink.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(sink.messages))
	}

	message := sink.messages[0]
	input, err := application.DecodeCandidateEventMessage(
		testMockCandidateTopic,
		application.InboundMessage{
			Topic:     message.Topic,
			Key:       message.Key,
			Value:     message.Value,
			Timestamp: message.Timestamp,
		},
	)
	if err != nil {
		t.Fatalf("DecodeCandidateEventMessage() error = %v", err)
	}

	if input.EventID != "mock:object-a:1" {
		t.Fatalf("EventID = %q, want %q", input.EventID, "mock:object-a:1")
	}
	if input.EventType != application.CandidateEventTypeCreated {
		t.Fatalf("EventType = %v, want created", input.EventType)
	}
	if input.ObjectID != "object-a" {
		t.Fatalf("ObjectID = %q, want %q", input.ObjectID, "object-a")
	}
	if input.CandidateRevision != mockCandidateRevision {
		t.Fatalf("CandidateRevision = %d, want %d", input.CandidateRevision, mockCandidateRevision)
	}
	if input.LightCurveRevision != mockLightCurveRevision {
		t.Fatalf("LightCurveRevision = %d, want %d", input.LightCurveRevision, mockLightCurveRevision)
	}
	if input.EligibleEpochCount != 3 {
		t.Fatalf("EligibleEpochCount = %d, want 3", input.EligibleEpochCount)
	}
	if !input.OccurredAt.Equal(occurredAt) {
		t.Fatalf("OccurredAt = %v, want %v", input.OccurredAt, occurredAt)
	}
	if input.Producer != mockCandidateProducer {
		t.Fatalf("Producer = %q, want %q", input.Producer, mockCandidateProducer)
	}
	if input.UpstreamPipelineVersion != mockUpstreamPipelineVersion {
		t.Fatalf(
			"UpstreamPipelineVersion = %q, want %q",
			input.UpstreamPipelineVersion,
			mockUpstreamPipelineVersion,
		)
	}

	var event classificationv1.CandidateEvent
	if err := proto.Unmarshal(message.Value, &event); err != nil {
		t.Fatalf("proto.Unmarshal() error = %v", err)
	}

	if event.GetQualityPolicyVersion() != mockQualityPolicyVersion {
		t.Fatalf(
			"QualityPolicyVersion = %q, want %q",
			event.GetQualityPolicyVersion(),
			mockQualityPolicyVersion,
		)
	}
	if event.GetTraceContext() == nil {
		t.Fatal("TraceContext = nil")
	}
}

func TestCandidateEventPublisherRoundRobinsAndRepeatsExactMessage(t *testing.T) {
	t.Parallel()

	dataset := candidatePublisherTestDataset()
	occurredAt := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)
	sink := &recordingMessagePublisher{}

	publisher, err := newCandidateEventPublisher(
		dataset,
		testMockCandidateTopic,
		12.5,
		occurredAt,
		sink,
	)
	if err != nil {
		t.Fatalf("newCandidateEventPublisher() error = %v", err)
	}

	if publisher.interval != 80*time.Millisecond {
		t.Fatalf("interval = %v, want %v", publisher.interval, 80*time.Millisecond)
	}

	for i := 0; i < 3; i++ {
		if err := publisher.publishNext(context.Background()); err != nil {
			t.Fatalf("publishNext(%d) error = %v", i, err)
		}
	}

	gotKeys := []string{
		string(sink.messages[0].Key),
		string(sink.messages[1].Key),
		string(sink.messages[2].Key),
	}
	wantKeys := []string{"object-a", "object-b", "object-a"}

	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("published keys = %#v, want %#v", gotKeys, wantKeys)
	}

	first := sink.messages[0]
	repeated := sink.messages[2]

	if !bytes.Equal(first.Value, repeated.Value) {
		t.Fatal("repeated object payload differs from original payload")
	}
	if !bytes.Equal(first.Key, repeated.Key) {
		t.Fatal("repeated object key differs from original key")
	}
	if !first.Timestamp.Equal(repeated.Timestamp) {
		t.Fatalf(
			"repeated timestamp = %v, want %v",
			repeated.Timestamp,
			first.Timestamp,
		)
	}
}

func TestCandidateEventPublisherDoesNotAdvanceAfterPublishFailure(t *testing.T) {
	t.Parallel()

	dataset := candidatePublisherTestDataset()
	expectedErr := errors.New("Kafka unavailable")
	sink := &recordingMessagePublisher{err: expectedErr}

	publisher, err := newCandidateEventPublisher(
		dataset,
		testMockCandidateTopic,
		1,
		time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC),
		sink,
	)
	if err != nil {
		t.Fatalf("newCandidateEventPublisher() error = %v", err)
	}

	err = publisher.publishNext(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("errors.Is(error, expectedErr) = false; error=%v", err)
	}
	if publisher.next != 0 {
		t.Fatalf("next = %d, want 0 after failed publish", publisher.next)
	}

	sink.err = nil

	if err := publisher.publishNext(context.Background()); err != nil {
		t.Fatalf("publishNext() after recovery error = %v", err)
	}
	if got := string(sink.messages[0].Key); got != "object-a" {
		t.Fatalf("published key = %q, want %q", got, "object-a")
	}
}

func TestNewCandidateEventPublisherRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	dataset := candidatePublisherTestDataset()
	sink := &recordingMessagePublisher{}
	occurredAt := time.Date(2026, 8, 10, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		dataset   lightCurveDataset
		topic     string
		rate      float64
		occurred  time.Time
		publisher application.MessagePublisher
	}{
		{
			name:      "empty dataset",
			dataset:   lightCurveDataset{},
			topic:     testMockCandidateTopic,
			rate:      1,
			occurred:  occurredAt,
			publisher: sink,
		},
		{
			name:      "blank topic",
			dataset:   dataset,
			topic:     " ",
			rate:      1,
			occurred:  occurredAt,
			publisher: sink,
		},
		{
			name:      "zero rate",
			dataset:   dataset,
			topic:     testMockCandidateTopic,
			rate:      0,
			occurred:  occurredAt,
			publisher: sink,
		},
		{
			name:      "zero occurred at",
			dataset:   dataset,
			topic:     testMockCandidateTopic,
			rate:      1,
			publisher: sink,
		},
		{
			name:     "nil publisher",
			dataset:  dataset,
			topic:    testMockCandidateTopic,
			rate:     1,
			occurred: occurredAt,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := newCandidateEventPublisher(
				test.dataset,
				test.topic,
				test.rate,
				test.occurred,
				test.publisher,
			); err == nil {
				t.Fatal("newCandidateEventPublisher() error = nil")
			}
		})
	}
}

func candidatePublisherTestDataset() lightCurveDataset {
	qualityPolicyVersion := mockQualityPolicyVersion

	return lightCurveDataset{
		objectIDs: []string{"object-a", "object-b"},
		revisions: map[string]domain.LightCurveRevision{
			"object-a": {
				ObjectID:             "object-a",
				Revision:             mockLightCurveRevision,
				EligibleEpochCount:   3,
				QualityPolicyVersion: &qualityPolicyVersion,
				Epochs: []domain.LightCurveEpoch{
					{ObservationTime: 1, Magnitude: 10, MagnitudeError: 0.1},
					{ObservationTime: 2, Magnitude: 11, MagnitudeError: 0.1},
					{ObservationTime: 3, Magnitude: 12, MagnitudeError: 0.1},
				},
			},
			"object-b": {
				ObjectID:             "object-b",
				Revision:             mockLightCurveRevision,
				EligibleEpochCount:   4,
				QualityPolicyVersion: &qualityPolicyVersion,
				Epochs: []domain.LightCurveEpoch{
					{ObservationTime: 1, Magnitude: 20, MagnitudeError: 0.1},
					{ObservationTime: 2, Magnitude: 21, MagnitudeError: 0.1},
					{ObservationTime: 3, Magnitude: 22, MagnitudeError: 0.1},
					{ObservationTime: 4, Magnitude: 23, MagnitudeError: 0.1},
				},
			},
		},
	}
}

type recordingMessagePublisher struct {
	messages []application.OutboundMessage
	err      error
}

func (publisher *recordingMessagePublisher) Publish(
	_ context.Context,
	message application.OutboundMessage,
) error {
	if publisher.err != nil {
		return publisher.err
	}

	cloned := message
	cloned.Key = append([]byte(nil), message.Key...)
	cloned.Value = append([]byte(nil), message.Value...)
	cloned.Headers = append([]application.MessageHeader(nil), message.Headers...)

	publisher.messages = append(publisher.messages, cloned)
	return nil
}
