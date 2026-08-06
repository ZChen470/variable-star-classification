package application

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ClassificationCommandRetryHandler 为 Classification Worker 增加有限快速重试
//
// retryDelays 中的每个元素表示下一次重试前的等待时间
// 因此总尝试次数为
//
// 1 + len(retryDelays)
//
// 它只重试结构化的 RETRYABLE WorkerError
//   - 成功：立即返回 nil；
//   - PERMANENT / CANCELLED：原样返回；
//   - 非结构化错误：原样返回；
//   - RETRYABLE 耗尽：返回最后一次错误，不提交原始 offset；
//   - 等待期间 Context 取消：返回 CANCELLED WorkerError
type ClassificationCommandRetryHandler struct {
	next        MessageHandler
	retryDelays []time.Duration
}

var _ MessageHandler = (*ClassificationCommandRetryHandler)(nil)

func NewClassificationCommandRetryHandler(next MessageHandler, retryDelays []time.Duration) (*ClassificationCommandRetryHandler, error) {
	if next == nil {
		return nil, errors.New("classification worker handler must not be nil")
	}
	if len(retryDelays) == 0 {
		return nil, errors.New("classification command retry delays must be empty")
	}

	for _, delay := range retryDelays {
		if delay < 0 {
			return nil, fmt.Errorf("classification command retry delay %d must not be negative", delay)
		}
	}

	return &ClassificationCommandRetryHandler{
		next:        next,
		retryDelays: retryDelays,
	}, nil
}

func (handler *ClassificationCommandRetryHandler) Handle(ctx context.Context, message InboundMessage) error {
	if ctx == nil {
		return errors.New("handle classification command retry: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	for attempt := 0; ; attempt++ {
		err := handler.next.Handle(ctx, message)
		if err == nil {
			return nil
		}

		var workerError *ClassificationWorkerError
		if !errors.As(err, &workerError) || workerError == nil || workerError.Class != ClassificationWorkerErrorClassRetryable {
			return err
		}

		// retryDelays 的数量就是允许进行的额外重试次数
		if attempt >= len(handler.retryDelays) {
			return err
		}

		if waitErr := waitClassificationCommandRetry(ctx, handler.retryDelays[attempt]); waitErr != nil {
			return WrapClassificationWorkerError(ClassificationWorkerOperationRetryWait, waitErr)
		}
	}
}

func waitClassificationCommandRetry(ctx context.Context, delay time.Duration) error {
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
