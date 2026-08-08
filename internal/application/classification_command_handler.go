package application

import "time"

// NewClassificationCommandHandler 组装 Classification Command 的完整处理链
//
//	Command DLQ
//	    └── finite retry
//	            └── classification worker
//
// 该顺序确保：
//   - 只对 Worker 返回的 RETRYABLE 错误进行有限快速重试；
//   - PERMANENT 错误在重试层之外进入 Command DLQ；
//   - DLQ 发布失败不会触发整个 Worker 流程的内部重试。
func NewClassificationCommandHandler(
	worker MessageHandler,
	retryDelays []time.Duration,
	dlqTopic string,
	dlqPublisher MessagePublisher,
) (MessageHandler, error) {
	return NewClassificationCommandHandlerWithObserver(
		worker,
		retryDelays,
		dlqTopic,
		dlqPublisher,
		nil,
	)
}

// NewClassificationCommandHandlerWithObserver 与
// NewClassificationCommandHandler 保持完全相同的处理语义，只额外向 observer
// 报告已经实际发生的 Retry / DLQ 运行事件。
func NewClassificationCommandHandlerWithObserver(
	worker MessageHandler,
	retryDelays []time.Duration,
	dlqTopic string,
	dlqPublisher MessagePublisher,
	observer ClassificationCommandObserver,
) (MessageHandler, error) {
	retryHandler, err :=
		NewClassificationCommandRetryHandlerWithObserver(
			worker,
			retryDelays,
			observer,
		)
	if err != nil {
		return nil, err
	}

	dlqHandler, err :=
		NewClassificationCommandDLQHandlerWithObserver(
			retryHandler,
			dlqTopic,
			dlqPublisher,
			observer,
		)
	if err != nil {
		return nil, err
	}

	return dlqHandler, nil
}
