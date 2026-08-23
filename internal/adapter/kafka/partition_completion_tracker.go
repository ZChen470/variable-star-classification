package kafka

import (
	"errors"
	"sync"
)

var ErrPartitionNotInitialized = errors.New(
	"partition completion tracker: partition not initialized",
)

type partitionCompletionTracker struct {
	mu         sync.Mutex
	partitions map[partitionKey]*partitionCompletionState
}

type partitionKey struct {
	topic     string
	partition int32
}

type partitionCompletionState struct {
	nextCommitOffset int64
	completedOffsets map[int64]struct{}
}

func newPartitionCompletionTracker() *partitionCompletionTracker {
	return &partitionCompletionTracker{
		partitions: make(
			map[partitionKey]*partitionCompletionState,
		),
	}
}

func (tracker *partitionCompletionTracker) InitializePartition(
	topic string,
	partition int32,
	nextCommitOffset int64,
) error {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	key := partitionKey{
		topic:     topic,
		partition: partition,
	}

	if _, exists := tracker.partitions[key]; exists {
		return errors.New(
			"partition already initialized",
		)
	}

	tracker.partitions[key] = &partitionCompletionState{
		nextCommitOffset: nextCommitOffset,
		completedOffsets: make(
			map[int64]struct{},
		),
	}

	return nil
}

func (tracker *partitionCompletionTracker) MarkCompleted(
	topic string,
	partition int32,
	offset int64,
) (int64, error) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	key := partitionKey{
		topic:     topic,
		partition: partition,
	}

	state, ok := tracker.partitions[key]

	if !ok {
		return 0, ErrPartitionNotInitialized
	}

	state.completedOffsets[offset] = struct{}{}

	for {
		if _, exists := state.completedOffsets[state.nextCommitOffset]; !exists {
			break
		}

		delete(
			state.completedOffsets,
			state.nextCommitOffset,
		)

		state.nextCommitOffset++
	}

	return state.nextCommitOffset, nil
}
