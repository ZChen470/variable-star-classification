package kafka

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeAsyncConsumer struct {
	mu          sync.Mutex
	fetches     []kgo.Fetches
	pollMax     []int
	commits     [][]*kgo.Record
	commitErr   error
	allowCount  int
	pollStarted chan struct{}
}

func (consumer *fakeAsyncConsumer) PollRecords(
	ctx context.Context,
	maxRecords int,
) kgo.Fetches {
	consumer.mu.Lock()
	consumer.pollMax = append(consumer.pollMax, maxRecords)
	if len(consumer.fetches) > 0 {
		fetches := consumer.fetches[0]
		consumer.fetches = consumer.fetches[1:]
		consumer.mu.Unlock()
		return fetches
	}
	consumer.mu.Unlock()

	if consumer.pollStarted != nil {
		select {
		case consumer.pollStarted <- struct{}{}:
		default:
		}
	}

	<-ctx.Done()
	return kgo.Fetches{}
}

func (consumer *fakeAsyncConsumer) CommitRecords(
	_ context.Context,
	records ...*kgo.Record,
) error {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	copyOfRecords := append([]*kgo.Record(nil), records...)
	consumer.commits = append(consumer.commits, copyOfRecords)
	return consumer.commitErr
}

func (consumer *fakeAsyncConsumer) AllowRebalance() {
	consumer.mu.Lock()
	consumer.allowCount++
	consumer.mu.Unlock()
}

func fetchesWithRecords(records ...*kgo.Record) kgo.Fetches {
	byPartition := make(map[int32][]*kgo.Record)
	for _, record := range records {
		byPartition[record.Partition] = append(byPartition[record.Partition], record)
	}

	partitionIDs := make([]int, 0, len(byPartition))
	for partition := range byPartition {
		partitionIDs = append(partitionIDs, int(partition))
	}
	sort.Ints(partitionIDs)

	partitions := make([]kgo.FetchPartition, 0, len(byPartition))
	for _, partitionID := range partitionIDs {
		partition := int32(partitionID)
		partitions = append(partitions, kgo.FetchPartition{
			Partition: partition,
			Records:   byPartition[partition],
		})
	}

	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      records[0].Topic,
		Partitions: partitions,
	}}}}
}

func TestAsyncConsumerRunnerProcessesDifferentKeysConcurrently(t *testing.T) {
	t.Parallel()

	records := []*kgo.Record{
		{Topic: "topic", Partition: 0, Offset: 100, Key: []byte("A")},
		{Topic: "topic", Partition: 0, Offset: 101, Key: []byte("B")},
	}
	consumer := &fakeAsyncConsumer{fetches: []kgo.Fetches{fetchesWithRecords(records...)}}

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	handler := application.MessageHandlerFunc(func(context.Context, application.InboundMessage) error {
		current := active.Add(1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	})

	runner, err := newAsyncConsumerRunner(consumer, handler, NewRebalanceYield(), 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("records did not start concurrently")
		}
	}
	close(release)

	deadline := time.After(time.Second)
	for {
		consumer.mu.Lock()
		committed := len(consumer.commits) == 1
		consumer.mu.Unlock()
		if committed {
			break
		}
		select {
		case <-deadline:
			t.Fatal("batch was not committed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if maxActive.Load() != 2 {
		t.Fatalf("max active = %d, want 2", maxActive.Load())
	}

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if got := consumer.pollMax[0]; got != 2 {
		t.Fatalf("PollRecords max = %d, want 2", got)
	}
	if got := consumer.commits[0][0].Offset; got != 101 {
		t.Fatalf("committed record offset = %d, want 101", got)
	}
}

func TestAsyncConsumerRunnerSerializesSameKeyInPollOrder(t *testing.T) {
	t.Parallel()

	records := []*kgo.Record{
		{Topic: "topic", Partition: 0, Offset: 100, Key: []byte("same")},
		{Topic: "topic", Partition: 0, Offset: 101, Key: []byte("same")},
	}
	consumer := &fakeAsyncConsumer{fetches: []kgo.Fetches{fetchesWithRecords(records...)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var order []int64
	handler := application.MessageHandlerFunc(func(_ context.Context, message application.InboundMessage) error {
		mu.Lock()
		order = append(order, message.Offset)
		mu.Unlock()
		if message.Offset == 100 {
			close(firstStarted)
			<-releaseFirst
		}
		return nil
	})

	runner, err := newAsyncConsumerRunner(consumer, handler, NewRebalanceYield(), 2)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-firstStarted
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	if len(order) != 1 {
		t.Fatalf("same-key second record started early: %v", order)
	}
	mu.Unlock()
	close(releaseFirst)

	deadline := time.After(time.Second)
	for {
		mu.Lock()
		complete := len(order) == 2
		mu.Unlock()
		if complete {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second record did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAsyncConsumerRunnerCommitsOnlyContinuousSuccessPrefix(t *testing.T) {
	t.Parallel()

	expected := errors.New("offset 101 failed")
	records := []*kgo.Record{
		{Topic: "topic", Partition: 0, Offset: 100, Key: []byte("A")},
		{Topic: "topic", Partition: 0, Offset: 101, Key: []byte("B")},
		{Topic: "topic", Partition: 0, Offset: 102, Key: []byte("C")},
	}
	consumer := &fakeAsyncConsumer{fetches: []kgo.Fetches{fetchesWithRecords(records...)}}
	offset100Done := make(chan struct{})
	offset102Done := make(chan struct{})
	handler := application.MessageHandlerFunc(func(_ context.Context, message application.InboundMessage) error {
		switch message.Offset {
		case 100:
			close(offset100Done)
			return nil
		case 101:
			<-offset100Done
			<-offset102Done
			return expected
		case 102:
			<-offset100Done
			close(offset102Done)
			return nil
		}
		panic("unexpected offset")
	})
	runner, err := newAsyncConsumerRunner(consumer, handler, NewRebalanceYield(), 3)
	if err != nil {
		t.Fatal(err)
	}

	err = runner.Run(context.Background())
	if !errors.Is(err, expected) {
		t.Fatalf("Run() error = %v, want %v", err, expected)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.commits) != 1 || len(consumer.commits[0]) != 1 {
		t.Fatalf("commits = %#v, want one partition watermark", consumer.commits)
	}
	if got := consumer.commits[0][0].Offset; got != 100 {
		t.Fatalf("committed record offset = %d, want 100", got)
	}
}

func TestAsyncConsumerRunnerDoesNotCommitYieldedBatch(t *testing.T) {
	t.Parallel()

	yield := NewRebalanceYield()
	consumer := &fakeAsyncConsumer{fetches: []kgo.Fetches{fetchesWithRecords(
		&kgo.Record{Topic: "topic", Partition: 0, Offset: 100, Key: []byte("A")},
	)}}
	started := make(chan struct{})
	handler := application.MessageHandlerFunc(func(ctx context.Context, _ application.InboundMessage) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	runner, err := newAsyncConsumerRunner(consumer, handler, yield, 1)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- runner.Run(context.Background()) }()
	<-started
	yield.Request()

	select {
	case err := <-done:
		if !errors.Is(err, ErrRebalanceYielded) {
			t.Fatalf("Run() error = %v, want ErrRebalanceYielded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not yield")
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.commits) != 0 {
		t.Fatalf("yielded batch commits = %d, want 0", len(consumer.commits))
	}
	if consumer.allowCount != 1 {
		t.Fatalf("AllowRebalance calls = %d, want 1", consumer.allowCount)
	}
}

func TestAsyncConsumerRunnerRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	handler := application.MessageHandlerFunc(func(context.Context, application.InboundMessage) error { return nil })
	if _, err := newAsyncConsumerRunner(nil, handler, NewRebalanceYield(), 1); err == nil {
		t.Fatal("nil consumer accepted")
	}
	if _, err := newAsyncConsumerRunner(&fakeAsyncConsumer{}, nil, NewRebalanceYield(), 1); err == nil {
		t.Fatal("nil handler accepted")
	}
	if _, err := newAsyncConsumerRunner(&fakeAsyncConsumer{}, handler, nil, 1); err == nil {
		t.Fatal("nil yield accepted")
	}
	if _, err := newAsyncConsumerRunner(&fakeAsyncConsumer{}, handler, NewRebalanceYield(), 0); err == nil {
		t.Fatal("zero concurrency accepted")
	}
}
