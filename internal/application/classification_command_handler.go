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
func NewClassificationCommandHandler(worker MessageHandler, retryDelays []time.Duration, dlqTopic string, dlqPublisher MessagePublisher) (MessageHandler, error) {
	retryHandler, err := NewClassificationCommandRetryHandler(worker, retryDelays)
	if err != nil {
		return nil, err
	}

	dlqHandler, err := NewClassificationCommandDLQHandler(retryHandler, dlqTopic, dlqPublisher)
	if err != nil {
		return nil, err
	}

	return dlqHandler, nil
}
