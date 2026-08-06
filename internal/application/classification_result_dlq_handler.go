package application

import (
	"context"
	"errors"
	"fmt"
)

// ClassificationResultDLQHandler 为 Result Writer 增加永久错误 DLQ 处置。
//
// 它不处理数据库临时错误，也不执行快速重试。
type ClassificationResultDLQHandler struct {
	next      MessageHandler
	dlqTopic  string
	publisher MessagePublisher
}

var _ MessageHandler = (*ClassificationResultDLQHandler)(nil)

func NewClassificationResultDLQHandler(
	next MessageHandler,
	dlqTopic string,
	publisher MessagePublisher,
) (*ClassificationResultDLQHandler, error) {
	if next == nil {
		return nil, errors.New(
			"classification result writer handler must not be nil",
		)
	}

	if dlqTopic == "" {
		return nil, errors.New(
			"classification result DLQ topic must not be empty",
		)
	}

	if publisher == nil {
		return nil, errors.New(
			"classification result DLQ publisher must not be nil",
		)
	}

	return &ClassificationResultDLQHandler{
		next:      next,
		dlqTopic:  dlqTopic,
		publisher: publisher,
	}, nil
}

func (handler *ClassificationResultDLQHandler) Handle(
	ctx context.Context,
	message InboundMessage,
) error {
	if handler == nil {
		return errors.New(
			"handle classification result DLQ: nil handler",
		)
	}

	if ctx == nil {
		return errors.New(
			"handle classification result DLQ: nil context",
		)
	}

	if handler.next == nil {
		return errors.New(
			"handle classification result DLQ: nil writer handler",
		)
	}

	if handler.publisher == nil {
		return errors.New(
			"handle classification result DLQ: nil publisher",
		)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	err := handler.next.Handle(ctx, message)
	if err == nil {
		return nil
	}

	var permanentError *PermanentClassificationResultError
	if !errors.As(err, &permanentError) {
		// Repository 临时错误、Context 取消或其他非永久错误，
		// 不进入 Result DLQ。
		return err
	}

	dlqMessage, buildErr :=
		BuildClassificationResultDLQMessage(
			handler.dlqTopic,
			message,
			permanentError,
		)
	if buildErr != nil {
		return fmt.Errorf(
			"build classification result DLQ: %w",
			buildErr,
		)
	}

	if publishErr := handler.publisher.Publish(
		ctx,
		dlqMessage,
	); publishErr != nil {
		return fmt.Errorf(
			"publish classification result DLQ: %w",
			publishErr,
		)
	}

	// 原始 Result 已由 DLQ 安全接收，外层可以提交其 offset。
	return nil
}
