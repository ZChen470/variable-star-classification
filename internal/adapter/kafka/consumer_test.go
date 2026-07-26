package kafka

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerRunnerProcessesAndCommitsRecords(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	first := &kgo.Record{
		Topic:     "astro.candidate.events.v1",
		Partition: 2,
		Offset:    10,
		Key:       []byte("OBJECT-001"),
		Value:     []byte{0x01},
		Timestamp: time.Date(
			2026,
			time.July,
			26,
			12,
			0,
			0,
			0,
			time.UTC,
		),
	}

	second := &kgo.Record{
		Topic:     first.Topic,
		Partition: first.Partition,
		Offset:    11,
		Key:       []byte("OBJECT-001"),
		Value:     []byte{0x02},
	}

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(first, second),
		},
		cancel:             cancel,
		cancelAfterCommits: 2,
	}

	var handled []application.InboundMessage

	runner := newConsumerRunner(
		consumer,
		application.MessageHandlerFunc(
			func(
				_ context.Context,
				message application.InboundMessage,
			) error {
				handled = append(handled, message)
				return nil
			},
		),
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run(): %v", err)
	}

	if len(handled) != 2 {
		t.Fatalf(
			"handled message count = %d, want 2",
			len(handled),
		)
	}

	if handled[0].Topic != first.Topic {
		t.Fatalf(
			"first topic = %q, want %q",
			handled[0].Topic,
			first.Topic,
		)
	}

	if handled[0].Partition != first.Partition {
		t.Fatalf(
			"first partition = %d, want %d",
			handled[0].Partition,
			first.Partition,
		)
	}

	if handled[0].Offset != first.Offset {
		t.Fatalf(
			"first offset = %d, want %d",
			handled[0].Offset,
			first.Offset,
		)
	}

	if len(consumer.committed) != 2 {
		t.Fatalf(
			"committed record count = %d, want 2",
			len(consumer.committed),
		)
	}

	if consumer.committed[0] != first {
		t.Fatal("first committed record is not the first fetched record")
	}

	if consumer.committed[1] != second {
		t.Fatal("second committed record is not the second fetched record")
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance call count = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func TestConsumerRunnerDoesNotCommitHandlerFailure(
	t *testing.T,
) {
	handlerErr := errors.New("message rejected")

	record := &kgo.Record{
		Topic:     "astro.candidate.events.v1",
		Partition: 0,
		Offset:    20,
		Key:       []byte("OBJECT-002"),
		Value:     []byte{0x01},
	}

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
	}

	runner := newConsumerRunner(
		consumer,
		application.MessageHandlerFunc(
			func(
				context.Context,
				application.InboundMessage,
			) error {
				return handlerErr
			},
		),
	)

	err := runner.Run(context.Background())

	if !errors.Is(err, handlerErr) {
		t.Fatalf(
			"Run() error = %v, want wrapped %v",
			err,
			handlerErr,
		)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf(
			"committed record count = %d, want 0",
			len(consumer.committed),
		)
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance call count = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func TestConsumerRunnerReturnsCommitFailure(
	t *testing.T,
) {
	commitErr := errors.New("commit unavailable")

	record := &kgo.Record{
		Topic:     "astro.candidate.events.v1",
		Partition: 1,
		Offset:    30,
		Key:       []byte("OBJECT-003"),
		Value:     []byte{0x01},
	}

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
		commitErr: commitErr,
	}

	runner := newConsumerRunner(
		consumer,
		application.MessageHandlerFunc(
			func(
				context.Context,
				application.InboundMessage,
			) error {
				return nil
			},
		),
	)

	err := runner.Run(context.Background())

	if !errors.Is(err, commitErr) {
		t.Fatalf(
			"Run() error = %v, want wrapped %v",
			err,
			commitErr,
		)
	}
}

func TestConsumerRunnerReturnsFetchFailure(
	t *testing.T,
) {
	fetchErr := errors.New("fetch authorization failed")

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			kgo.NewErrFetch(fetchErr),
		},
	}

	runner := newConsumerRunner(
		consumer,
		application.MessageHandlerFunc(
			func(
				context.Context,
				application.InboundMessage,
			) error {
				return nil
			},
		),
	)

	err := runner.Run(context.Background())

	if !errors.Is(err, fetchErr) {
		t.Fatalf(
			"Run() error = %v, want wrapped %v",
			err,
			fetchErr,
		)
	}
}

func TestConsumerRunnerStopsOnCanceledContext(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	consumer := &fakeConsumerClient{}

	runner := newConsumerRunner(
		consumer,
		application.MessageHandlerFunc(
			func(
				context.Context,
				application.InboundMessage,
			) error {
				t.Fatal("handler was called")
				return nil
			},
		),
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run(): %v", err)
	}

	if consumer.pollCalls != 0 {
		t.Fatalf(
			"PollFetches call count = %d, want 0",
			consumer.pollCalls,
		)
	}
}

func TestInboundMessageFromRecordClonesData(
	t *testing.T,
) {
	record := &kgo.Record{
		Topic:     "topic-v1",
		Partition: 4,
		Offset:    40,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers: []kgo.RecordHeader{
			{
				Key:   "trace-id",
				Value: []byte("trace"),
			},
		},
	}

	message := inboundMessageFromRecord(record)

	message.Key[0] = 'K'
	message.Value[0] = 'V'
	message.Headers[0].Value[0] = 'T'

	if !bytes.Equal(record.Key, []byte("key")) {
		t.Fatalf(
			"record key mutated: %q",
			record.Key,
		)
	}

	if !bytes.Equal(record.Value, []byte("value")) {
		t.Fatalf(
			"record value mutated: %q",
			record.Value,
		)
	}

	if !bytes.Equal(
		record.Headers[0].Value,
		[]byte("trace"),
	) {
		t.Fatalf(
			"record header mutated: %q",
			record.Headers[0].Value,
		)
	}
}

func TestNewConsumerRunnerRequiresManualCommitOptions(
	t *testing.T,
) {
	handler := application.MessageHandlerFunc(
		func(
			context.Context,
			application.InboundMessage,
		) error {
			return nil
		},
	)

	autoCommitClient, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.ConsumerGroup("test-group"),
		kgo.ConsumeTopics("test-topic"),
	)
	if err != nil {
		t.Fatalf("NewClient(auto commit): %v", err)
	}
	defer autoCommitClient.Close()

	_, err = NewConsumerRunner(
		autoCommitClient,
		handler,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "DisableAutoCommit") {
		t.Fatalf(
			"NewConsumerRunner() error = %v, want DisableAutoCommit error",
			err,
		)
	}

	unblockedClient, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.ConsumerGroup("test-group"),
		kgo.ConsumeTopics("test-topic"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("NewClient(unblocked): %v", err)
	}
	defer unblockedClient.Close()

	_, err = NewConsumerRunner(
		unblockedClient,
		handler,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "BlockRebalanceOnPoll") {
		t.Fatalf(
			"NewConsumerRunner() error = %v, want BlockRebalanceOnPoll error",
			err,
		)
	}

	validClient, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.ConsumerGroup("test-group"),
		kgo.ConsumeTopics("test-topic"),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatalf("NewClient(valid): %v", err)
	}
	defer validClient.CloseAllowingRebalance()

	runner, err := NewConsumerRunner(
		validClient,
		handler,
	)
	if err != nil {
		t.Fatalf("NewConsumerRunner(valid): %v", err)
	}

	if runner == nil {
		t.Fatal("NewConsumerRunner(valid) returned nil")
	}
}

type fakeConsumerClient struct {
	fetches []kgo.Fetches

	committed []*kgo.Record
	commitErr error

	allowRebalanceCalls int
	pollCalls           int

	cancel             context.CancelFunc
	cancelAfterCommits int
}

func (consumer *fakeConsumerClient) PollFetches(
	ctx context.Context,
) kgo.Fetches {
	consumer.pollCalls++

	if len(consumer.fetches) > 0 {
		fetches := consumer.fetches[0]
		consumer.fetches = consumer.fetches[1:]
		return fetches
	}

	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (consumer *fakeConsumerClient) CommitRecords(
	_ context.Context,
	records ...*kgo.Record,
) error {
	if consumer.commitErr != nil {
		return consumer.commitErr
	}

	consumer.committed = append(
		consumer.committed,
		records...,
	)

	if consumer.cancel != nil &&
		consumer.cancelAfterCommits > 0 &&
		len(consumer.committed) >=
			consumer.cancelAfterCommits {
		consumer.cancel()
	}

	return nil
}

func (consumer *fakeConsumerClient) AllowRebalance() {
	consumer.allowRebalanceCalls++
}

func fetchWithRecords(
	records ...*kgo.Record,
) kgo.Fetches {
	if len(records) == 0 {
		return nil
	}

	return kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: records[0].Topic,
					Partitions: []kgo.FetchPartition{
						{
							Partition: records[0].Partition,
							Records:   records,
						},
					},
				},
			},
		},
	}
}
