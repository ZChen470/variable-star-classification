package kafka

import (
	"errors"
	"testing"
)

func TestPartitionCompletionTrackerWaitsForGap(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()
	for _, offset := range []int64{100, 101, 102} {
		if err := tracker.Track("topic", 0, offset); err != nil {
			t.Fatal(err)
		}
	}

	if offset, advanced, err := tracker.MarkCompleted("topic", 0, 102); err != nil {
		t.Fatal(err)
	} else if advanced || offset != 0 {
		t.Fatalf("completion advanced to %d, want no advance", offset)
	}

	if offset, advanced, err := tracker.MarkCompleted("topic", 0, 100); err != nil {
		t.Fatal(err)
	} else if !advanced || offset != 100 {
		t.Fatalf("completion advanced=%v offset=%d, want true/100", advanced, offset)
	}

	if offset, advanced, err := tracker.MarkCompleted("topic", 0, 101); err != nil {
		t.Fatal(err)
	} else if !advanced || offset != 102 {
		t.Fatalf("completion advanced=%v offset=%d, want true/102", advanced, offset)
	}
}

func TestPartitionCompletionTrackerAllowsNumericOffsetGaps(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()
	for _, offset := range []int64{100, 105, 109} {
		if err := tracker.Track("topic", 0, offset); err != nil {
			t.Fatal(err)
		}
	}

	_, _, _ = tracker.MarkCompleted("topic", 0, 109)
	_, _, _ = tracker.MarkCompleted("topic", 0, 105)
	offset, advanced, err := tracker.MarkCompleted("topic", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !advanced || offset != 109 {
		t.Fatalf("completion advanced=%v offset=%d, want true/109", advanced, offset)
	}
}

func TestPartitionCompletionTrackerSeparatesPartitions(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()
	_ = tracker.Track("topic", 0, 100)
	_ = tracker.Track("topic", 1, 500)

	offset, advanced, err := tracker.MarkCompleted("topic", 1, 500)
	if err != nil {
		t.Fatal(err)
	}
	if !advanced || offset != 500 {
		t.Fatalf("completion advanced=%v offset=%d, want true/500", advanced, offset)
	}
}

func TestPartitionCompletionTrackerRejectsUnknownAndDuplicateOffsets(t *testing.T) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()
	if err := tracker.Track("topic", 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Track("topic", 0, 100); !errors.Is(err, ErrOffsetAlreadyTracked) {
		t.Fatalf("Track() error = %v, want ErrOffsetAlreadyTracked", err)
	}
	if _, _, err := tracker.MarkCompleted("topic", 0, 101); !errors.Is(err, ErrOffsetNotTracked) {
		t.Fatalf("MarkCompleted() error = %v, want ErrOffsetNotTracked", err)
	}
}
