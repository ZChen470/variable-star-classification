package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ClassificationResultRetryHandler retries transient ClassificationResult
// processing failures until processing succeeds or the record context is
// cancelled.
//
// PermanentClassificationResultError is returned immediately so an outer
// ClassificationResultDLQHandler can publish the original Result to DLQ.
type ClassificationResultRetryHandler struct {
	next        MessageHandler
	retryDelays []time.Duration
	logger      *slog.Logger
}

var _ MessageHandler = (*ClassificationResultRetryHandler)(nil)

func NewClassificationResultRetryHandler(
	next MessageHandler,
	retryDelays []time.Duration,
) (*ClassificationResultRetryHandler, error) {
	if next == nil {
		return nil, errors.New("classification result writer handler must not be nil")
	}
	if len(retryDelays) == 0 {
		return nil, errors.New("classification result retry delays must not be empty")
	}

	for _, delay := range retryDelays {
		if delay < 0 {
			return nil, fmt.Errorf(
				"classification result retry delay %s must not be negative",
				delay,
			)
		}
	}

	return &ClassificationResultRetryHandler{
		next:        next,
		retryDelays: retryDelays,
		logger:      slog.Default(),
	}, nil
}

func (handler *ClassificationResultRetryHandler) Handle(
	ctx context.Context,
	message InboundMessage,
) error {
	if handler == nil || handler.next == nil {
		return errors.New("handle classification result retry: nil handler")
	}
	if ctx == nil {
		return errors.New("handle classification result retry: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	logger := handler.logger
	if logger == nil {
		logger = slog.Default()
	}

	for attempt := 1; ; attempt++ {
		err := handler.next.Handle(ctx, message)
		if err == nil {
			return nil
		}

		var permanentError *PermanentClassificationResultError
		if errors.As(err, &permanentError) {
			return err
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		if errors.Is(err, context.Canceled) {
			return err
		}

		delay := classificationResultRetryDelay(
			handler.retryDelays,
			attempt-1,
		)

		logger.WarnContext(
			ctx,
			"classification result retry scheduled",
			"operation", "classification_result_retry",
			"attempt", attempt,
			"next_attempt", attempt+1,
			"retry_delay_ms", delay.Milliseconds(),
			"error_class", "TRANSIENT",
			"kafka_topic", message.Topic,
			"kafka_partition", message.Partition,
			"kafka_offset", message.Offset,
		)

		if waitErr := waitClassificationResultRetry(ctx, delay); waitErr != nil {
			logger.InfoContext(
				ctx,
				"classification result retry wait cancelled",
				"operation", "classification_result_retry_wait_cancelled",
				"attempt", attempt,
				"kafka_topic", message.Topic,
				"kafka_partition", message.Partition,
				"kafka_offset", message.Offset,
			)

			return waitErr
		}
	}
}

func classificationResultRetryDelay(
	retryDelays []time.Duration,
	retryIndex int,
) time.Duration {
	if retryIndex < len(retryDelays) {
		return retryDelays[retryIndex]
	}

	return retryDelays[len(retryDelays)-1]
}

func waitClassificationResultRetry(
	ctx context.Context,
	delay time.Duration,
) error {
	if delay == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
