package application

import (
	"context"
	"errors"
	"fmt"
)

// CandidateHandler 编排 CandidateEvent 解码、策略判断、Command 发布和永久错误 DLQ 发布
type CandidateHandler struct {
	candidateTopic    string
	commandTopic      string
	candidateDLQTopic string
	policy            ClassificationPolicyV1
	publisher         MessagePublisher
}

var _ MessageHandler = (*CandidateHandler)(nil)

// NewCandidateHandler 创建 CandidateEvent 应用层 Handler
func NewCandidateHandler(candidateTopic string, commandTopic string, candidateDLQTopic string, policy ClassificationPolicyV1, publisher MessagePublisher) (*CandidateHandler, error) {
	// 1 校验数据
	if candidateTopic == "" {
		return nil, errors.New("candidate topic must not be empty")
	}
	if commandTopic == "" {
		return nil, errors.New(
			"classification command topic must not be empty",
		)
	}
	if candidateDLQTopic == "" {
		return nil, errors.New("candidate DLQ topic must not be empty")
	}
	if policy.modelBundleVersion == "" {
		return nil, errors.New("classification policy is not configured")
	}
	if publisher == nil {
		return nil, errors.New("candidate message publisher must not be nil")
	}

	return &CandidateHandler{
		candidateTopic:    candidateTopic,
		commandTopic:      commandTopic,
		candidateDLQTopic: candidateDLQTopic,
		policy:            policy,
		publisher:         publisher,
	}, nil
}

// Handle 处理一条 CandidateEvent 入站消息。
//
// 返回 nil 后，ConsumerRunner 才会提交原始 Kafka offset。
func (handler *CandidateHandler) Handle(ctx context.Context, message InboundMessage) error {
	if ctx == nil {
		return errors.New("handle candidate message: nil context")
	}
	if handler == nil {
		return errors.New("handle candidate message: nil handler")
	}
	if handler.publisher == nil {
		return errors.New("handle candidate message: nil publisher")
	}

	input, err := DecodeCandidateEventMessage(handler.candidateTopic, message)
	if err != nil {
		var permanentErr *PermanentCandidateMessageError
		if !errors.As(err, &permanentErr) {
			return fmt.Errorf("decode candidate event: %w", err)
		}

		dlqMessage, buildErr := BuildCandidateDLQMessage(handler.candidateDLQTopic, message, permanentErr)
		if buildErr != nil {
			return fmt.Errorf("build candidate DLQ message: %w", buildErr)
		}

		if publishErr := handler.publisher.Publish(ctx, dlqMessage); publishErr != nil {
			return fmt.Errorf("publish candidate DLQ message: %w", publishErr)
		}

		// 如果成功发布到 DLQ 就不用管
		return nil
	}

	decision, err := handler.policy.Evaluate(input)
	if err != nil {
		return fmt.Errorf("evaluate classification policy: %w", err)
	}
	if !decision.ShouldClassify {
		return nil
	}

	commandMessage, err := BuildClassificationCommandMessage(handler.commandTopic, input, decision, message.Headers)
	if err != nil {
		return fmt.Errorf("build classification command message: %w", err)
	}
	if err := handler.publisher.Publish(ctx, commandMessage); err != nil {
		return fmt.Errorf("publish classification command message: %w", err)
	}

	return nil
}
