package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPublisherPublish(t *testing.T) {
	producer := &fakeSyncProducer{}
	publisher := newPublisher(producer)

	timestamp := time.Date(
		2026,
		time.July,
		25,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	message := application.OutboundMessage{
		Topic:     "astro.classification.results.v1",
		Key:       []byte("OBJECT-001"),
		Value:     []byte{0x01, 0x02, 0x03},
		Timestamp: timestamp,
		Headers: []application.MessageHeader{
			{
				Key:   "trace-id",
				Value: []byte("trace-001"),
			},
			{
				Key:   "correlation-id",
				Value: []byte("correlation-001"),
			},
		},
	}

	if err := publisher.Publish(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Publish(): %v", err)
	}

	if len(producer.records) != 1 {
		t.Fatalf(
			"produced record count = %d, want 1",
			len(producer.records),
		)
	}

	record := producer.records[0]

	if record.Topic != message.Topic {
		t.Fatalf(
			"topic = %q, want %q",
			record.Topic,
			message.Topic,
		)
	}

	if !reflect.DeepEqual(record.Key, message.Key) {
		t.Fatalf(
			"key = %q, want %q",
			record.Key,
			message.Key,
		)
	}

	if !reflect.DeepEqual(record.Value, message.Value) {
		t.Fatalf(
			"value = %v, want %v",
			record.Value,
			message.Value,
		)
	}

	if !record.Timestamp.Equal(timestamp) {
		t.Fatalf(
			"timestamp = %v, want %v",
			record.Timestamp,
			timestamp,
		)
	}

	wantHeaders := []kgo.RecordHeader{
		{
			Key:   "trace-id",
			Value: []byte("trace-001"),
		},
		{
			Key:   "correlation-id",
			Value: []byte("correlation-001"),
		},
	}

	if !reflect.DeepEqual(record.Headers, wantHeaders) {
		t.Fatalf(
			"headers = %#v, want %#v",
			record.Headers,
			wantHeaders,
		)
	}
}

func TestPublisherReturnsProduceError(t *testing.T) {
	produceErr := errors.New("broker unavailable")

	publisher := newPublisher(
		&fakeSyncProducer{
			err: produceErr,
		},
	)

	err := publisher.Publish(
		context.Background(),
		application.OutboundMessage{
			Topic: "astro.classification.results.v1",
			Key:   []byte("OBJECT-001"),
			Value: []byte{0x01},
		},
	)

	if !errors.Is(err, produceErr) {
		t.Fatalf(
			"Publish() error = %v, want wrapped %v",
			err,
			produceErr,
		)
	}
}

func TestPublisherAllowsEmptyKeyAndValue(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "nil key and nil value",
			key:   nil,
			value: nil,
		},
		{
			name:  "empty non-nil key and value",
			key:   []byte{},
			value: []byte{},
		},
		{
			name:  "nil key with value",
			key:   nil,
			value: []byte("original-value"),
		},
		{
			name:  "key with nil value",
			key:   []byte("original-key"),
			value: nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			producer := &fakeSyncProducer{}
			publisher := newPublisher(producer)

			err := publisher.Publish(
				context.Background(),
				application.OutboundMessage{
					Topic: "astro.candidate.events.dlq.v1",
					Key:   test.key,
					Value: test.value,
				},
			)
			if err != nil {
				t.Fatalf("Publish() returned error: %v", err)
			}

			if len(producer.records) != 1 {
				t.Fatalf(
					"produced record count = %d, want 1",
					len(producer.records),
				)
			}

			record := producer.records[0]

			if !reflect.DeepEqual(record.Key, test.key) {
				t.Fatalf(
					"record key = %#v, want %#v",
					record.Key,
					test.key,
				)
			}

			if !reflect.DeepEqual(record.Value, test.value) {
				t.Fatalf(
					"record value = %#v, want %#v",
					record.Value,
					test.value,
				)
			}
		})
	}
}

func TestPublisherRejectsInvalidMessage(t *testing.T) {
	testCases := []struct {
		name      string
		message   application.OutboundMessage
		errorPart string
	}{
		{
			name: "empty topic",
			message: application.OutboundMessage{
				Key:   []byte("OBJECT-001"),
				Value: []byte{0x01},
			},
			errorPart: "topic is empty",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			publisher := newPublisher(
				&fakeSyncProducer{},
			)

			err := publisher.Publish(
				context.Background(),
				testCase.message,
			)
			if err == nil {
				t.Fatal("Publish() error = nil")
			}

			if !strings.Contains(
				err.Error(),
				testCase.errorPart,
			) {
				t.Fatalf(
					"Publish() error = %q, want substring %q",
					err,
					testCase.errorPart,
				)
			}
		})
	}
}

func TestPublisherPreservesArbitraryHeaders(t *testing.T) {
	t.Parallel()

	producer := &fakeSyncProducer{}
	publisher := newPublisher(producer)

	message := application.OutboundMessage{
		Topic: "astro.candidate.events.dlq.v1",
		Headers: []application.MessageHeader{
			{
				Key:   "",
				Value: nil,
			},
			{
				Key:   "",
				Value: []byte{},
			},
			{
				Key:   "normal-header",
				Value: []byte("value"),
			},
		},
	}

	if err := publisher.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish() returned error: %v", err)
	}

	if len(producer.records) != 1 {
		t.Fatalf(
			"produced record count = %d, want 1",
			len(producer.records),
		)
	}

	wantHeaders := []kgo.RecordHeader{
		{
			Key:   "",
			Value: nil,
		},
		{
			Key:   "",
			Value: []byte{},
		},
		{
			Key:   "normal-header",
			Value: []byte("value"),
		},
	}

	if !reflect.DeepEqual(producer.records[0].Headers, wantHeaders) {
		t.Fatalf(
			"headers = %#v, want %#v",
			producer.records[0].Headers,
			wantHeaders,
		)
	}
}

func TestPublisherRejectsNilDependencies(t *testing.T) {
	var nilPublisher *Publisher

	err := nilPublisher.Publish(
		context.Background(),
		application.OutboundMessage{},
	)
	if err == nil {
		t.Fatal("nil Publisher Publish() error = nil")
	}

	publisher := NewPublisher(nil)

	err = publisher.Publish(
		context.Background(),
		application.OutboundMessage{},
	)
	if err == nil {
		t.Fatal("nil client Publish() error = nil")
	}

	err = newPublisher(&fakeSyncProducer{}).Publish(
		nil,
		application.OutboundMessage{},
	)
	if err == nil {
		t.Fatal("nil context Publish() error = nil")
	}
}

type fakeSyncProducer struct {
	records []*kgo.Record
	err     error
}

func (producer *fakeSyncProducer) ProduceSync(
	_ context.Context,
	records ...*kgo.Record,
) kgo.ProduceResults {
	producer.records = append(
		producer.records,
		records...,
	)

	results := make(
		kgo.ProduceResults,
		0,
		len(records),
	)

	for _, record := range records {
		results = append(
			results,
			kgo.ProduceResult{
				Record: record,
				Err:    producer.err,
			},
		)
	}

	return results
}
