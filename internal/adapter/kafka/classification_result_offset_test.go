package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	offsetSemanticsCommandTopic = "astro.classification.commands.v1"

	offsetSemanticsResultTopic = "astro.classification.results.v1"

	offsetSemanticsCommandDLQTopic = "astro.classification.commands.dlq.v1"
)

// TestClassificationResultPublishSuccessCommitsCommandOffset 冻结：
//
//	Result Publish 成功
//	→ Worker 返回 nil
//	→ Command Handler 返回 nil
//	→ ConsumerRunner 提交 Command Offset
func TestClassificationResultPublishSuccessCommitsCommandOffset(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	record := &kgo.Record{
		Topic:     offsetSemanticsCommandTopic,
		Partition: 2,
		Offset:    41,
		Key:       []byte("OBJ-OFFSET-001"),
		Value:     []byte{0x01, 0x02},

		Timestamp: time.Date(
			2026,
			time.August,
			6,
			20,
			0,
			0,
			0,
			time.UTC,
		),
	}

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},

		cancel:             cancel,
		cancelAfterCommits: 1,
	}

	resultPublisher :=
		&classificationOffsetTestPublisher{}

	worker :=
		&classificationResultPublishBoundaryHandler{
			publisher: resultPublisher,
		}

	dlqPublisher :=
		&classificationOffsetTestPublisher{}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			offsetSemanticsCommandDLQTopic,
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandHandler() error = %v",
			err,
		)
	}

	runner := newConsumerRunner(
		consumer,
		handler,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf(
			"ConsumerRunner.Run() error = %v",
			err,
		)
	}

	if worker.calls != 1 {
		t.Fatalf(
			"worker call count = %d, want 1",
			worker.calls,
		)
	}

	if len(resultPublisher.messages) != 1 {
		t.Fatalf(
			"result publish count = %d, want 1",
			len(resultPublisher.messages),
		)
	}

	published := resultPublisher.messages[0]

	if published.Topic != offsetSemanticsResultTopic {
		t.Fatalf(
			"result topic = %q, want %q",
			published.Topic,
			offsetSemanticsResultTopic,
		)
	}

	if len(consumer.committed) != 1 {
		t.Fatalf(
			"committed record count = %d, want 1",
			len(consumer.committed),
		)
	}

	if consumer.committed[0] != record {
		t.Fatal(
			"committed record is not the consumed Command record",
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"Command DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}
}

// TestClassificationResultPublishFailureDoesNotCommitCommandOffset 冻结：
//
//	Result Publish 持续失败
//	→ Worker 持续返回 RETRYABLE
//	→ Command Handler 持续重试且不进入 DLQ
//	→ Context 取消后停止处理
//	→ ConsumerRunner 不提交 Command Offset
func TestClassificationResultPublishFailureDoesNotCommitCommandOffset(t *testing.T) {
	publishCause := errors.New("classification result Kafka unavailable")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	record := &kgo.Record{
		Topic:     offsetSemanticsCommandTopic,
		Partition: 3,
		Offset:    52,
		Key:       []byte("OBJ-OFFSET-002"),
		Value:     []byte{0x03, 0x04},
	}

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
	}

	resultPublisher := &classificationOffsetTestPublisher{
		err:              publishCause,
		cancel:           cancel,
		cancelAfterCalls: 3,
	}

	worker := &classificationResultPublishBoundaryHandler{
		publisher: resultPublisher,
	}

	dlqPublisher := &classificationOffsetTestPublisher{}
	handler, err := application.NewClassificationCommandHandler(
		worker,
		[]time.Duration{0, 0},
		offsetSemanticsCommandDLQTopic,
		dlqPublisher,
	)
	if err != nil {
		t.Fatalf("NewClassificationCommandHandler() error = %v", err)
	}

	runner := newConsumerRunner(consumer, handler)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("ConsumerRunner.Run() error = %v, want nil on context cancellation", err)
	}

	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("context error = %v, want context.Canceled", ctx.Err())
	}

	if worker.calls != 3 {
		t.Fatalf("worker call count = %d, want 3", worker.calls)
	}

	if len(resultPublisher.messages) != 3 {
		t.Fatalf(
			"result publish count = %d, want 3",
			len(resultPublisher.messages),
		)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf(
			"committed record count = %d, want 0",
			len(consumer.committed),
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"Command DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance call count = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

// classificationResultPublishBoundaryHandler 只模拟 Worker 最后一段
// “Result Message → MessagePublisher”的边界。
//
// 完整 Worker 的 Command 解码、输入准备、推理和 Result 构造已经由
// application 层测试覆盖；本测试专门验证发布结果如何影响 Offset。
type classificationResultPublishBoundaryHandler struct {
	publisher application.MessagePublisher
	calls     int
}

func (
	handler *classificationResultPublishBoundaryHandler,
) Handle(
	ctx context.Context,
	message application.InboundMessage,
) error {
	handler.calls++

	err := handler.publisher.Publish(
		ctx,
		application.OutboundMessage{
			Topic: offsetSemanticsResultTopic,
			Key: append(
				[]byte(nil),
				message.Key...,
			),
			Value:     []byte{0x01},
			Timestamp: message.Timestamp,
		},
	)
	if err != nil {
		return application.WrapClassificationWorkerError(
			application.
				ClassificationWorkerOperationPublishResult,
			err,
		)
	}

	return nil
}

type classificationOffsetTestPublisher struct {
	err              error
	messages         []application.OutboundMessage
	cancel           context.CancelFunc
	cancelAfterCalls int
}

func (publisher *classificationOffsetTestPublisher) Publish(
	_ context.Context,
	message application.OutboundMessage,
) error {
	publisher.messages = append(publisher.messages, message)

	if publisher.cancel != nil &&
		publisher.cancelAfterCalls > 0 &&
		len(publisher.messages) == publisher.cancelAfterCalls {
		publisher.cancel()
	}

	return publisher.err
}
