package kafka

import (
	"errors"
	"fmt"
)

var (
	ErrOffsetNotTracked = errors.New(
		"partition completion tracker: offset not tracked",
	)
	ErrOffsetAlreadyTracked = errors.New(
		"partition completion tracker: offset already tracked",
	)
)

// partitionCompletionTracker tracks the records that were actually returned
// by Kafka. It deliberately does not assume that numeric offsets are
// contiguous: compacted and transactional logs may contain offset gaps.
//
// The tracker is owned by one poll batch, so it does not need a mutex.
type partitionCompletionTracker struct {
	partitions map[partitionKey]*partitionCompletionState
}

type partitionKey struct {
	topic     string
	partition int32
}

type partitionCompletionState struct {
	offsets   []int64
	index     map[int64]int
	completed map[int64]struct{}
	next      int
}

func newPartitionCompletionTracker() *partitionCompletionTracker {
	return &partitionCompletionTracker{
		partitions: make(map[partitionKey]*partitionCompletionState),
	}
}

// Track records the per-partition poll order before processing begins.
func (tracker *partitionCompletionTracker) Track(
	topic string,
	partition int32,
	offset int64,
) error {
	if tracker == nil {
		return errors.New("partition completion tracker is nil")
	}

	key := partitionKey{topic: topic, partition: partition}
	state, ok := tracker.partitions[key]
	if !ok {
		state = &partitionCompletionState{
			index:     make(map[int64]int),
			completed: make(map[int64]struct{}),
		}
		tracker.partitions[key] = state
	}

	if _, exists := state.index[offset]; exists {
		return fmt.Errorf(
			"%w: topic %q partition %d offset %d",
			ErrOffsetAlreadyTracked,
			topic,
			partition,
			offset,
		)
	}

	if count := len(state.offsets); count > 0 && offset < state.offsets[count-1] {
		return fmt.Errorf(
			"track Kafka offset out of order: topic %q partition %d offset %d after %d",
			topic,
			partition,
			offset,
			state.offsets[count-1],
		)
	}

	state.index[offset] = len(state.offsets)
	state.offsets = append(state.offsets, offset)

	return nil
}

// MarkCompleted marks one tracked record complete. If this closes the gap at
// the front of the partition queue, completedOffset is the highest actual
// record offset in the newly committable prefix and advanced is true.
func (tracker *partitionCompletionTracker) MarkCompleted(
	topic string,
	partition int32,
	offset int64,
) (completedOffset int64, advanced bool, err error) {
	if tracker == nil {
		return 0, false, errors.New("partition completion tracker is nil")
	}

	key := partitionKey{topic: topic, partition: partition}
	state, ok := tracker.partitions[key]
	if !ok {
		return 0, false, fmt.Errorf(
			"%w: topic %q partition %d offset %d",
			ErrOffsetNotTracked,
			topic,
			partition,
			offset,
		)
	}

	if _, ok := state.index[offset]; !ok {
		return 0, false, fmt.Errorf(
			"%w: topic %q partition %d offset %d",
			ErrOffsetNotTracked,
			topic,
			partition,
			offset,
		)
	}

	state.completed[offset] = struct{}{}
	start := state.next

	for state.next < len(state.offsets) {
		candidate := state.offsets[state.next]
		if _, exists := state.completed[candidate]; !exists {
			break
		}

		delete(state.completed, candidate)
		state.next++
		completedOffset = candidate
	}

	return completedOffset, state.next > start, nil
}
