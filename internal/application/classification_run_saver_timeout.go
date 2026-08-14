package application

import (
	"context"
	"errors"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

// TimeoutClassificationRunSaver bounds one ClassificationRun persistence
// operation without changing persistence semantics.
type TimeoutClassificationRunSaver struct {
	next    ClassificationRunSaver
	timeout time.Duration
}

var _ ClassificationRunSaver = (*TimeoutClassificationRunSaver)(nil)

func NewTimeoutClassificationRunSaver(
	next ClassificationRunSaver,
	timeout time.Duration,
) (*TimeoutClassificationRunSaver, error) {
	if next == nil {
		return nil, errors.New("classification run saver must not be nil")
	}
	if timeout <= 0 {
		return nil, errors.New("classification run save timeout must be positive")
	}

	return &TimeoutClassificationRunSaver{
		next:    next,
		timeout: timeout,
	}, nil
}

func (saver *TimeoutClassificationRunSaver) SaveRunAndMaybeAdvanceCurrent(
	ctx context.Context,
	run domain.ClassificationRun,
) (SaveRunResult, error) {
	if saver == nil || saver.next == nil {
		return SaveRunResult{}, errors.New("save classification run with timeout: nil saver")
	}
	if ctx == nil {
		return SaveRunResult{}, errors.New("save classification run with timeout: nil context")
	}
	if err := ctx.Err(); err != nil {
		return SaveRunResult{}, err
	}

	saveContext, cancel := context.WithTimeout(ctx, saver.timeout)
	defer cancel()

	return saver.next.SaveRunAndMaybeAdvanceCurrent(saveContext, run)
}
