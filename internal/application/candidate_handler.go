package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// CandidateHandler 编排 CandidateEvent 解码、策略判断、Command 发布和永久错误 DLQ 发布
type CandidateHandler struct {
	candidateTopic    string
	commandTopic      string
	candidateDLQTopic string
	policy            ClassificationPolicyV1
	publisher         MessagePublisher
	logger            *slog.Logger
}

var _ MessageHandler = (*CandidateHandler)(nil)

// NewCandidateHandler 创建 CandidateEvent 应用层 Handler
func NewCandidateHandler(
	candidateTopic string,
	commandTopic string,
	candidateDLQTopic string,
	policy ClassificationPolicyV1,
	publisher MessagePublisher,
) (*CandidateHandler, error) {
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
		logger:            slog.Default(),
	}, nil
}

// Handle 处理一条 CandidateEvent 入站消息。
//
// 返回 nil 后，ConsumerRunner 才会提交原始 Kafka offset。
func (handler *CandidateHandler) Handle(
	ctx context.Context,
	message InboundMessage,
) error {
	if ctx == nil {
		return errors.New("handle candidate message: nil context")
	}
	if handler == nil {
		return errors.New("handle candidate message: nil handler")
	}
	if handler.publisher == nil {
		return errors.New("handle candidate message: nil publisher")
	}

	input, err := DecodeCandidateEventMessage(
		handler.candidateTopic,
		message,
	)
	if err != nil {
		var permanentErr *PermanentCandidateMessageError
		if !errors.As(err, &permanentErr) {
			handler.logger.ErrorContext(
				ctx,
				"candidate decode failed",
				"operation", "candidate_decode",
				"kafka_topic", message.Topic,
				"kafka_partition", message.Partition,
				"kafka_offset", message.Offset,
				"error", err,
			)

			return fmt.Errorf("decode candidate event: %w", err)
		}

		handler.logger.WarnContext(
			ctx,
			"candidate decode failed",
			"operation", "candidate_decode",
			"error_code", string(permanentErr.Code),
			"error_class", "PERMANENT",
			"error_field", permanentErr.Field,
			"kafka_topic", message.Topic,
			"kafka_partition", message.Partition,
			"kafka_offset", message.Offset,
			"error", permanentErr,
		)

		dlqMessage, buildErr := BuildCandidateDLQMessage(
			handler.candidateDLQTopic,
			message,
			permanentErr,
		)
		if buildErr != nil {
			handler.logger.ErrorContext(
				ctx,
				"candidate DLQ message build failed",
				"operation", "candidate_dlq_build",
				"error_code", string(permanentErr.Code),
				"error_class", "PERMANENT",
				"kafka_topic", message.Topic,
				"kafka_partition", message.Partition,
				"kafka_offset", message.Offset,
				"error", buildErr,
			)

			return fmt.Errorf(
				"build candidate DLQ message: %w",
				buildErr,
			)
		}

		if publishErr := handler.publisher.Publish(
			ctx,
			dlqMessage,
		); publishErr != nil {
			handler.logger.ErrorContext(
				ctx,
				"candidate DLQ publish failed",
				"operation", "candidate_dlq_publish",
				"error_code", string(permanentErr.Code),
				"kafka_topic", message.Topic,
				"kafka_partition", message.Partition,
				"kafka_offset", message.Offset,
				"dlq_topic", handler.candidateDLQTopic,
				"error", publishErr,
			)

			return fmt.Errorf(
				"publish candidate DLQ message: %w",
				publishErr,
			)
		}

		handler.logger.WarnContext(
			ctx,
			"candidate published to DLQ",
			"operation", "candidate_dlq_publish",
			"error_code", string(permanentErr.Code),
			"error_class", "PERMANENT",
			"kafka_topic", message.Topic,
			"kafka_partition", message.Partition,
			"kafka_offset", message.Offset,
			"dlq_topic", handler.candidateDLQTopic,
		)

		return nil
	}

	handler.logger.InfoContext(
		ctx,
		"candidate received",
		"operation", "candidate_received",
		"event_id", input.EventID,
		"object_id", input.ObjectID,
		"candidate_revision", input.CandidateRevision,
		"light_curve_revision", input.LightCurveRevision,
		"trace_id", input.TraceContext.TraceID,
		"correlation_id", input.TraceContext.CorrelationID,
		"causation_id", input.TraceContext.CausationID,
		"kafka_topic", message.Topic,
		"kafka_partition", message.Partition,
		"kafka_offset", message.Offset,
	)

	decision, err := handler.policy.Evaluate(input)
	if err != nil {
		handler.logger.ErrorContext(
			ctx,
			"classification policy evaluation failed",
			"operation", "classification_policy",
			"object_id", input.ObjectID,
			"candidate_revision", input.CandidateRevision,
			"light_curve_revision", input.LightCurveRevision,
			"error", err,
		)

		return fmt.Errorf(
			"evaluate classification policy: %w",
			err,
		)
	}

	if !decision.ShouldClassify {
		return nil
	}

	commandMessage, err := BuildClassificationCommandMessage(
		handler.commandTopic,
		input,
		decision,
		message.Headers,
	)
	if err != nil {
		handler.logger.ErrorContext(
			ctx,
			"classification command build failed",
			"operation", "classification_command_build",
			"object_id", input.ObjectID,
			"candidate_revision", input.CandidateRevision,
			"light_curve_revision", input.LightCurveRevision,
			"model_bundle_version", decision.ModelBundleVersion,
			"error", err,
		)

		return fmt.Errorf(
			"build classification command message: %w",
			err,
		)
	}

	if err := handler.publisher.Publish(
		ctx,
		commandMessage,
	); err != nil {
		handler.logger.ErrorContext(
			ctx,
			"classification command publish failed",
			"operation", "classification_command_publish",
			"object_id", input.ObjectID,
			"candidate_revision", input.CandidateRevision,
			"light_curve_revision", input.LightCurveRevision,
			"model_bundle_version", decision.ModelBundleVersion,
			"kafka_topic", commandMessage.Topic,
			"error", err,
		)

		return fmt.Errorf(
			"publish classification command message: %w",
			err,
		)
	}

	handler.logger.InfoContext(
		ctx,
		"classification command published",
		"operation", "classification_command_publish",
		"object_id", input.ObjectID,
		"candidate_revision", input.CandidateRevision,
		"light_curve_revision", input.LightCurveRevision,
		"model_bundle_version", decision.ModelBundleVersion,
		"trace_id", input.TraceContext.TraceID,
		"correlation_id", input.TraceContext.CorrelationID,
		"causation_id", input.TraceContext.CausationID,
		"kafka_topic", commandMessage.Topic,
	)

	return nil
}
