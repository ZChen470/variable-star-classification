package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

const (
	// ClassificationResultErrorCodeRepositoryConflict 表示 Result
	// 与已有 ClassificationRun 的确定性身份发生冲突
	ClassificationResultErrorCodeRepositoryConflict ClassificationResultErrorCode = "RESULT_REPOSITORY_CONFLICT"
)

// ClassificationRunSaver 是 Result Writer 所需的最小持久化 Port
//
// ClassificationRepository 自动满足本接口，但 Writer 不依赖其查询方法
type ClassificationRunSaver interface {
	SaveRunAndMaybeAdvanceCurrent(
		ctx context.Context,
		run domain.ClassificationRun,
	) (SaveRunResult, error)
}

// ClassificationResultWriterHandler 消费已经发布的 ClassificationResult，
// 独立解码并将成功 Run 原子保存到 Repository
//
// 返回 nil 后，外层 Kafka ConsumerRunner 才可以提交 Result offset
type ClassificationResultWriterHandler struct {
	resultTopic string
	repository  ClassificationRunSaver
	logger      *slog.Logger
}

var _ MessageHandler = (*ClassificationResultWriterHandler)(nil)

func NewClassificationResultWriterHandler(
	resultTopic string,
	repository ClassificationRunSaver,
) (*ClassificationResultWriterHandler, error) {
	if resultTopic == "" {
		return nil, errors.New(
			"classification result topic must not be empty",
		)
	}

	if repository == nil {
		return nil, errors.New(
			"classification result repository must not be nil",
		)
	}

	return &ClassificationResultWriterHandler{
		resultTopic: resultTopic,
		repository:  repository,
		logger:      slog.Default(),
	}, nil
}

func (handler *ClassificationResultWriterHandler) Handle(
	ctx context.Context,
	message InboundMessage,
) error {
	if handler == nil {
		return errors.New(
			"write classification result: nil handler",
		)
	}

	if ctx == nil {
		return errors.New(
			"write classification result: nil context",
		)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	logger := handler.logger
	if logger == nil {
		logger = slog.Default()
	}

	input, err := DecodeClassificationResultMessage(
		handler.resultTopic,
		message,
	)
	if err != nil {
		return err
	}

	logger.InfoContext(
		ctx,
		"classification result received",
		"operation", "classification_result_received",
		"job_id", string(input.Run.JobID),
		"run_id", string(input.Run.RunID),
		"object_id", input.Run.ObjectID,
		"candidate_revision", input.Run.CandidateRevision,
		"light_curve_revision", input.Run.LightCurveRevision,
		"model_bundle_version",
		input.Run.Versions.ModelBundleVersion,
		"trace_id", input.TraceContext.TraceID,
		"correlation_id", input.TraceContext.CorrelationID,
		"causation_id", input.TraceContext.CausationID,
		"kafka_topic", message.Topic,
		"kafka_partition", message.Partition,
		"kafka_offset", message.Offset,
	)

	saveResult, err :=
		handler.repository.SaveRunAndMaybeAdvanceCurrent(
			ctx,
			input.Run,
		)

	if err == nil {
		logger.InfoContext(
			ctx,
			"classification run persisted",
			"operation", "classification_run_persist",
			"job_id", string(input.Run.JobID),
			"run_id", string(input.Run.RunID),
			"object_id", input.Run.ObjectID,
			"candidate_revision",
			input.Run.CandidateRevision,
			"light_curve_revision",
			input.Run.LightCurveRevision,
			"model_bundle_version",
			input.Run.Versions.ModelBundleVersion,
			"run_inserted", saveResult.RunInserted,
			"current_advanced",
			saveResult.CurrentAdvanced,
			"kafka_topic", message.Topic,
			"kafka_partition", message.Partition,
			"kafka_offset", message.Offset,
		)

		// 以下全部属于成功：
		//
		//   RunInserted=true,  CurrentAdvanced=true
		//   RunInserted=true,  CurrentAdvanced=false
		//   RunInserted=false, CurrentAdvanced=false
		//
		// 重复 Result 是幂等成功，不应阻止 offset 提交。
		return nil
	}

	if errors.Is(err, ErrClassificationRunConflict) {
		return &PermanentClassificationResultError{
			Code:  ClassificationResultErrorCodeRepositoryConflict,
			Field: "classification_run",
			Cause: err,
		}
	}

	// 数据库连接、事务或其他临时错误继续向外返回。
	// ConsumerRunner 因此不会提交当前 Result offset
	return fmt.Errorf(
		"save classification result: %w",
		err,
	)
}
