package application

import (
	"context"
	"errors"
	"log/slog"
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
	next      MessageHandler
	dlqTopic  string
	publisher MessagePublisher
	logger    *slog.Logger
	observer  ClassificationCommandObserver
}

var _ MessageHandler = (*ClassificationCommandDLQHandler)(nil)

func NewClassificationCommandDLQHandler(
	next MessageHandler,
	dlqTopic string,
	publisher MessagePublisher,
) (*ClassificationCommandDLQHandler, error) {
	return NewClassificationCommandDLQHandlerWithObserver(
		next,
		dlqTopic,
		publisher,
		nil,
	)
}

func NewClassificationCommandDLQHandlerWithObserver(
	next MessageHandler,
	dlqTopic string,
	publisher MessagePublisher,
	observer ClassificationCommandObserver,
) (*ClassificationCommandDLQHandler, error) {
	if next == nil {
		return nil, errors.New(
			"classification worker handler must not be nil",
		)
	}

	if dlqTopic == "" {
		return nil, errors.New(
			"classification command DLQ topic must not be empty",
		)
	}

	if publisher == nil {
		return nil, errors.New(
			"classification command DLQ publisher must not be empty",
		)
	}

	return &ClassificationCommandDLQHandler{
		next:      next,
		dlqTopic:  dlqTopic,
		publisher: publisher,
		logger:    slog.Default(),
		observer: classificationCommandObserverOrNoop(
			observer,
		),
	}, nil
}

func (handler *ClassificationCommandDLQHandler) Handle(
	ctx context.Context,
	message InboundMessage,
) error {
	if ctx == nil {
		return errors.New(
			"handle classification command DLQ: nil context",
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	logger := handler.logger
	if logger == nil {
		logger = slog.Default()
	}

	err := handler.next.Handle(ctx, message)
	if err == nil {
		return nil
	}

	var workerError *ClassificationWorkerError
	if !errors.As(err, &workerError) {
		return err
	}

	if workerError.Class !=
		ClassificationWorkerErrorClassPermanent {
		return err
	}

	dlqMessage, buildErr :=
		BuildClassificationCommandDLQMessage(
			handler.dlqTopic,
			message,
			workerError,
		)
	if buildErr != nil {
		wrappedErr := WrapClassificationWorkerError(
			ClassificationWorkerOperationBuildCommandDLQ,
			buildErr,
		)

		var dlqError *ClassificationWorkerError
		if errors.As(
			wrappedErr,
			&dlqError,
		) && dlqError != nil {
			logger.ErrorContext(
				ctx,
				"classification command DLQ build failed",
				"operation",
				"classification_command_dlq_build",
				"error_code", string(dlqError.Code),
				"error_class", dlqError.Class.String(),
				"worker_operation",
				string(dlqError.Operation),
				"source_error_code",
				string(workerError.Code),
				"source_worker_operation",
				string(workerError.Operation),
				"kafka_topic", message.Topic,
				"kafka_partition", message.Partition,
				"kafka_offset", message.Offset,
				"dlq_topic", handler.dlqTopic,
			)
		}

		return wrappedErr
	}

	if publishErr := handler.publisher.Publish(
		ctx,
		dlqMessage,
	); publishErr != nil {
		wrappedErr := WrapClassificationWorkerError(
			ClassificationWorkerOperationPublishCommandDLQ,
			publishErr,
		)

		var dlqError *ClassificationWorkerError
		if errors.As(
			wrappedErr,
			&dlqError,
		) && dlqError != nil {
			logger.ErrorContext(
				ctx,
				"classification command DLQ publish failed",
				"operation",
				"classification_command_dlq_publish",
				"error_code", string(dlqError.Code),
				"error_class", dlqError.Class.String(),
				"worker_operation",
				string(dlqError.Operation),
				"source_error_code",
				string(workerError.Code),
				"source_worker_operation",
				string(workerError.Operation),
				"kafka_topic", message.Topic,
				"kafka_partition", message.Partition,
				"kafka_offset", message.Offset,
				"dlq_topic", handler.dlqTopic,
			)
		}

		return wrappedErr
	}

	handler.observer.DLQPublished()

	logger.WarnContext(
		ctx,
		"classification command published to DLQ",
		"operation", "classification_command_dlq_publish",
		"error_code", string(workerError.Code),
		"error_class", workerError.Class.String(),
		"worker_operation", string(workerError.Operation),
		"kafka_topic", message.Topic,
		"kafka_partition", message.Partition,
		"kafka_offset", message.Offset,
		"dlq_topic", handler.dlqTopic,
	)

	// DLQ 已成功保存原始 Command，允许外层提交原始 offset。
	return nil
}
