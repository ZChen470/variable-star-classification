package application

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ClassificationWorkerHandler 编排一次 ClassificationCommand 的完整成功路径
//
// Command
//
//	→ 固定 Revision 输入准备
//	→ Serving Bundle 解析
//	→ 模型分类
//	→ ClassificationRun 构造
//	→ ClassificationResult 消息构造
//	→ Result 发布
//
// 本 Handler：
//   - 不直接写数据库；
//   - 不自行提交 Kafka offset；
//   - 不执行 Retry 或 DLQ；
//   - 不对依赖错误进行永久/可重试分类；
//   - 只有 Result 发布成功才返回 nil。
type ClassificationWorkerHandler struct {
	commandTopic string
	resultTopic  string

	inputPrepare          *ClassificationInputPreparer
	servingBundleResolver ServingBundleResolver
	classifier            VariableStarClassifier
	publisher             MessagePublisher

	now func() time.Time
}

var _ MessageHandler = (*ClassificationWorkerHandler)(nil)

// NewClassificationWorkerHandler 创建应用层 Classification Worker Handler
//
// now 由调用方注入：
//   - 生产装配可传 time.Now:
//   - 测试传固定时间函数，以保持 completed_at 和 消息字节确定
func NewClassificationWorkerHandler(
	commandTopic string,
	resultTopic string,
	inputPrepare *ClassificationInputPreparer,
	servingBundleResolver ServingBundleResolver,
	classifier VariableStarClassifier,
	publisher MessagePublisher,
	now func() time.Time,
) (*ClassificationWorkerHandler, error) {
	if commandTopic == "" {
		return nil, errors.New("classification command topic must not be empty")
	}
	if resultTopic == "" {
		return nil, errors.New("classification result topic must not be empty")
	}
	if inputPrepare == nil {
		return nil, errors.New("classification input preparer must not be nil")
	}
	if servingBundleResolver == nil {
		return nil, errors.New("serving bundle resolver must not be nil")
	}
	if classifier == nil {
		return nil, errors.New("variable star classifier must not be nil")
	}
	if publisher == nil {
		return nil, errors.New("classification result publisher must not be nil")
	}
	if now == nil {
		return nil, errors.New("classification worker clock must not be nil")
	}

	return &ClassificationWorkerHandler{
		commandTopic:          commandTopic,
		resultTopic:           resultTopic,
		inputPrepare:          inputPrepare,
		servingBundleResolver: servingBundleResolver,
		classifier:            classifier,
		publisher:             publisher,
		now:                   now,
	}, nil
}

// Handle 执行一条 ClassificationCommand 的完整成功路径
//
// 返回 nil 后，外层 ConsumerRunner 才能提交原始 command offset
// 任意步骤失败都会返回包装后的原始错误，且不会继续执行后续步骤
func (handler *ClassificationWorkerHandler) Handle(ctx context.Context, message InboundMessage) error {
	if ctx == nil {
		return errors.New("handle classification command: nil context")
	}
	if handler == nil {
		return errors.New("handle classification command: nil handler")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	command, err := DecodeClassificationCommandMessage(handler.commandTopic, message)
	if err != nil {
		return fmt.Errorf("decode classification command: %w", err)
	}

	prepared, err := handler.inputPrepare.Prepare(ctx, ClassificationInputPreparationRequest{
		ObjectID:                   command.ObjectID,
		LightCurveRevision:         command.LightCurveRevision,
		DeclaredEligibleEpochCount: command.DeclaredEligibleEpochCount,
		ModelBundleVersion:         command.ModelBundleVersion,
	})
	if err != nil {
		return fmt.Errorf("prepare classification input: %w", err)
	}

	servingBundle, err := handler.servingBundleResolver.ResolveServingBundle(ctx, command.ModelBundleVersion)
	if err != nil {
		return fmt.Errorf("resolve serving bundle: %w", err)
	}

	output, err := handler.classifier.Classify(ctx, prepared.Input)
	if err != nil {
		return fmt.Errorf("classify variable star: %w", err)
	}

	// 推理完后，Context 已取消，不再生成或发布 Result
	if err := ctx.Err(); err != nil {
		return err
	}

	run, err := BuildClassificationRun(
		ClassificationRunBuildRequest{
			JobID:             command.JobID,
			CandidateRevision: command.CandidateRevision,
			ExecutionMode:     command.ExecutionMode,
			Prepared:          prepared,
			ServingBundle:     servingBundle,
			Output:            output,
			CompletedAt:       handler.now(),
		},
	)
	if err != nil {
		return fmt.Errorf("build classification run: %w", err)
	}

	resultMessage, err := BuildClassificationResultMessage(handler.resultTopic, run, command.TraceContext, message.Headers)
	if err != nil {
		return fmt.Errorf("build classification result message: %w", err)
	}

	if err = handler.publisher.Publish(ctx, resultMessage); err != nil {
		return fmt.Errorf("publish classification result: %w", err)
	}

	return nil
}
