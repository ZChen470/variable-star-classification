package application

import (
	"context"
	"errors"
)

// ClassificationCommandDLQHandler 为 Classification Worker 增加永久错误
// Command DLQ 处置
//
// 它不执行重试：
//   - 成功：返回 nil
//   - PERMANENT：发布 DLQ，成功后返回 nil
//   - RETRYABLE / CANCELLED：原样返回
//   - DLQ 发布失败：返回错误，不允许提交原始 offset
type ClassificationCommandDLQHandler struct {
	next      MessageHandler // 下游 Handler
	dlqTopic  string
	publisher MessagePublisher
}

var _ MessageHandler = (*ClassificationCommandDLQHandler)(nil)

func NewClassificationCommandDLQHandler(next MessageHandler, dlqTopic string, publisher MessagePublisher) (*ClassificationCommandDLQHandler, error) {
	if next == nil {
		return nil, errors.New("classification worker handler must not be nil")
	}
	if dlqTopic == "" {
		return nil, errors.New("classification command DLQ topic must not be empty")
	}
	if publisher == nil {
		return nil, errors.New("classification command DLQ publisher must not be empty")
	}

	return &ClassificationCommandDLQHandler{
		next:      next,
		dlqTopic:  dlqTopic,
		publisher: publisher,
	}, nil
}

func (handler *ClassificationCommandDLQHandler) Handle(ctx context.Context, message InboundMessage) error {
	if ctx == nil {
		return errors.New("handle classification command DLQ: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	err := handler.next.Handle(ctx, message)
	if err == nil {
		return nil
	}

	var workerError *ClassificationWorkerError
	if !errors.As(err, &workerError) {
		return err
	}

	if workerError.Class != ClassificationWorkerErrorClassPermanent {
		return err
	}

	dlqMessage, buildErr := BuildClassificationCommandDLQMessage(handler.dlqTopic, message, workerError)
	if buildErr != nil {
		return WrapClassificationWorkerError(ClassificationWorkerOperationBuildCommandDLQ, buildErr)
	}

	if publishErr := handler.publisher.Publish(ctx, dlqMessage); publishErr != nil {
		return WrapClassificationWorkerError(ClassificationWorkerOperationPublishCommandDLQ, publishErr)
	}

	// DLQ 已成功保存原始 Command，允许外层提交原始 offset
	return nil
}
