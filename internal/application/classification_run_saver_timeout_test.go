package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

type timeoutClassificationRunSaverFake struct {
	save func(context.Context, domain.ClassificationRun) (SaveRunResult, error)
}

func (fake *timeoutClassificationRunSaverFake) SaveRunAndMaybeAdvanceCurrent(
	ctx context.Context,
	run domain.ClassificationRun,
) (SaveRunResult, error) {
	return fake.save(ctx, run)
}

func TestNewTimeoutClassificationRunSaverRejectsInvalidConfiguration(t *testing.T) {
	fake := &timeoutClassificationRunSaverFake{
		save: func(context.Context, domain.ClassificationRun) (SaveRunResult, error) {
			return SaveRunResult{}, nil
		},
	}

	tests := []struct {
		name    string
		next    ClassificationRunSaver
		timeout time.Duration
	}{
		{
			name:    "nil saver",
			next:    nil,
			timeout: time.Second,
		},
		{
			name:    "zero timeout",
			next:    fake,
			timeout: 0,
		},
		{
			name:    "negative timeout",
			next:    fake,
			timeout: -time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewTimeoutClassificationRunSaver(test.next, test.timeout); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}

func TestTimeoutClassificationRunSaverAppliesDeadline(t *testing.T) {
	const timeout = time.Second

	var deadlinePresent bool

	fake := &timeoutClassificationRunSaverFake{
		save: func(ctx context.Context, run domain.ClassificationRun) (SaveRunResult, error) {
			deadline, ok := ctx.Deadline()
			deadlinePresent = ok
			if !ok {
				t.Fatal("expected persistence context deadline")
			}

			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > timeout {
				t.Fatalf("unexpected persistence deadline remaining: %s", remaining)
			}

			return SaveRunResult{
				RunInserted:     true,
				CurrentAdvanced: true,
			}, nil
		},
	}

	saver, err := NewTimeoutClassificationRunSaver(fake, timeout)
	if err != nil {
		t.Fatalf("create timeout saver: %v", err)
	}

	result, err := saver.SaveRunAndMaybeAdvanceCurrent(
		context.Background(),
		domain.ClassificationRun{},
	)
	if err != nil {
		t.Fatalf("save classification run: %v", err)
	}
	if !deadlinePresent {
		t.Fatal("expected persistence context deadline")
	}
	if !result.RunInserted {
		t.Fatal("expected RunInserted")
	}
	if !result.CurrentAdvanced {
		t.Fatal("expected CurrentAdvanced")
	}
}

func TestTimeoutClassificationRunSaverStopsBlockedPersistence(t *testing.T) {
	const timeout = 50 * time.Millisecond

	fake := &timeoutClassificationRunSaverFake{
		save: func(ctx context.Context, run domain.ClassificationRun) (SaveRunResult, error) {
			<-ctx.Done()
			return SaveRunResult{}, ctx.Err()
		},
	}

	saver, err := NewTimeoutClassificationRunSaver(fake, timeout)
	if err != nil {
		t.Fatalf("create timeout saver: %v", err)
	}

	startedAt := time.Now()

	_, err = saver.SaveRunAndMaybeAdvanceCurrent(
		context.Background(),
		domain.ClassificationRun{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("blocked persistence was not cancelled promptly: %s", elapsed)
	}
}

func TestTimeoutClassificationRunSaverPreservesEarlierParentCancellation(t *testing.T) {
	called := false

	fake := &timeoutClassificationRunSaverFake{
		save: func(ctx context.Context, run domain.ClassificationRun) (SaveRunResult, error) {
			called = true
			return SaveRunResult{}, nil
		},
	}

	saver, err := NewTimeoutClassificationRunSaver(fake, time.Second)
	if err != nil {
		t.Fatalf("create timeout saver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = saver.SaveRunAndMaybeAdvanceCurrent(ctx, domain.ClassificationRun{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if called {
		t.Fatal("underlying saver must not be called for an already-cancelled context")
	}
}

func TestTimeoutClassificationRunSaverRejectsNilContext(t *testing.T) {
	fake := &timeoutClassificationRunSaverFake{
		save: func(context.Context, domain.ClassificationRun) (SaveRunResult, error) {
			return SaveRunResult{}, nil
		},
	}

	saver, err := NewTimeoutClassificationRunSaver(fake, time.Second)
	if err != nil {
		t.Fatalf("create timeout saver: %v", err)
	}

	if _, err := saver.SaveRunAndMaybeAdvanceCurrent(nil, domain.ClassificationRun{}); err == nil {
		t.Fatal("expected nil context error")
	}
}
