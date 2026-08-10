package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	mockCandidateRevision       int64 = 1
	mockCandidateProducer             = "lightcurve-mock-server"
	mockUpstreamPipelineVersion       = "mock-v1"
)

type candidateEventPublisher struct {
	messages  []application.OutboundMessage
	publisher application.MessagePublisher
	interval  time.Duration
	next      int
}

func newCandidateEventPublisher(
	dataset lightCurveDataset,
	topic string,
	ratePerSecond float64,
	occurredAt time.Time,
	publisher application.MessagePublisher,
) (*candidateEventPublisher, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("candidate topic must not be blank")
	}
	if publisher == nil {
		return nil, errors.New("candidate publisher must not be nil")
	}
	if ratePerSecond <= 0 || math.IsNaN(ratePerSecond) || math.IsInf(ratePerSecond, 0) {
		return nil, errors.New("candidate rate per second must be a positive finite number")
	}
	if occurredAt.IsZero() {
		return nil, errors.New("candidate occurred_at must not be zero")
	}

	interval := time.Duration(float64(time.Second) / ratePerSecond)
	if interval <= 0 {
		return nil, fmt.Errorf("candidate rate per second %v is too high", ratePerSecond)
	}

	objectIDs := dataset.ObjectIDs()
	if len(objectIDs) == 0 {
		return nil, errors.New("candidate dataset must contain at least one object")
	}

	occurredAt = occurredAt.UTC()
	messages := make([]application.OutboundMessage, 0, len(objectIDs))

	for _, objectID := range objectIDs {
		revision, ok := dataset.Revision(objectID, mockLightCurveRevision)
		if !ok {
			return nil, fmt.Errorf(
				"candidate dataset missing object_id=%q revision=%d",
				objectID,
				mockLightCurveRevision,
			)
		}

		message, err := buildCandidateEventMessage(topic, revision, occurredAt)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	return &candidateEventPublisher{
		messages:  messages,
		publisher: publisher,
		interval:  interval,
	}, nil
}

func buildCandidateEventMessage(
	topic string,
	revision domain.LightCurveRevision,
	occurredAt time.Time,
) (application.OutboundMessage, error) {
	timestamp := timestamppb.New(occurredAt.UTC())
	if err := timestamp.CheckValid(); err != nil {
		return application.OutboundMessage{}, fmt.Errorf("invalid candidate occurred_at: %w", err)
	}

	eventID := fmt.Sprintf("mock:%s:%d", revision.ObjectID, revision.Revision)

	var qualityPolicyVersion *string
	if revision.QualityPolicyVersion != nil {
		value := *revision.QualityPolicyVersion
		qualityPolicyVersion = &value
	}

	event := &classificationv1.CandidateEvent{
		EventId:                 eventID,
		EventType:               classificationv1.CandidateEventType_CANDIDATE_CREATED,
		ObjectId:                revision.ObjectID,
		CandidateRevision:       mockCandidateRevision,
		LightCurveRevision:      revision.Revision,
		EligibleEpochCount:      revision.EligibleEpochCount,
		OccurredAt:              timestamp,
		Producer:                mockCandidateProducer,
		UpstreamPipelineVersion: mockUpstreamPipelineVersion,
		QualityPolicyVersion:    qualityPolicyVersion,
		TraceContext: &classificationv1.TraceContext{
			TraceId:       "trace:" + eventID,
			CorrelationId: "correlation:" + eventID,
			CausationId:   "causation:" + eventID,
		},
	}

	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(event)
	if err != nil {
		return application.OutboundMessage{}, fmt.Errorf(
			"marshal CandidateEvent object_id=%q: %w",
			revision.ObjectID,
			err,
		)
	}

	return application.OutboundMessage{
		Topic:     topic,
		Key:       []byte(revision.ObjectID),
		Value:     value,
		Timestamp: occurredAt.UTC(),
	}, nil
}

func (publisher *candidateEventPublisher) publishNext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("publish CandidateEvent: nil context")
	}
	if publisher == nil || publisher.publisher == nil {
		return errors.New("publish CandidateEvent: nil publisher")
	}
	if len(publisher.messages) == 0 {
		return errors.New("publish CandidateEvent: no messages")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	message := publisher.messages[publisher.next]
	if err := publisher.publisher.Publish(ctx, message); err != nil {
		return fmt.Errorf(
			"publish CandidateEvent object_id=%q: %w",
			string(message.Key),
			err,
		)
	}

	publisher.next = (publisher.next + 1) % len(publisher.messages)
	return nil
}

func (publisher *candidateEventPublisher) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run CandidateEvent publisher: nil context")
	}
	if publisher == nil || publisher.interval <= 0 {
		return errors.New("run CandidateEvent publisher: invalid publisher")
	}

	ticker := time.NewTicker(publisher.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := publisher.publishNext(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}
