package kafka

import (
	"errors"
	"testing"
)

func TestPartitionCompletionTrackerRequiresInitialization(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	_, err := tracker.MarkCompleted(
		"topic",
		0,
		100,
	)

	if !errors.Is(
		err,
		ErrPartitionNotInitialized,
	) {
		t.Fatalf(
			"err=%v, want ErrPartitionNotInitialized",
			err,
		)
	}
}

func TestPartitionCompletionTrackerSequentialCompletion(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	err := tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	if err != nil {
		t.Fatalf(
			"initialize: %v",
			err,
		)
	}

	got, err := tracker.MarkCompleted(
		"topic",
		0,
		100,
	)

	if err != nil {
		t.Fatalf(
			"complete: %v",
			err,
		)
	}

	if got != 101 {
		t.Fatalf(
			"watermark=%d want=101",
			got,
		)
	}
}

func TestPartitionCompletionTrackerWaitsForGap(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	_ = tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	got, _ := tracker.MarkCompleted(
		"topic",
		0,
		101,
	)

	if got != 100 {
		t.Fatalf(
			"watermark=%d want=100",
			got,
		)
	}

	got, _ = tracker.MarkCompleted(
		"topic",
		0,
		100,
	)

	if got != 102 {
		t.Fatalf(
			"watermark=%d want=102",
			got,
		)
	}
}

func TestPartitionCompletionTrackerSeparatesPartitions(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	_ = tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	_ = tracker.InitializePartition(
		"topic",
		1,
		500,
	)

	got, _ := tracker.MarkCompleted(
		"topic",
		0,
		100,
	)

	if got != 101 {
		t.Fatalf(
			"partition0 watermark=%d",
			got,
		)
	}

	got, _ = tracker.MarkCompleted(
		"topic",
		1,
		500,
	)

	if got != 501 {
		t.Fatalf(
			"partition1 watermark=%d",
			got,
		)
	}
}
