package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaPublisherConsumerIntegration(t *testing.T) {
	brokers := kafkaIntegrationBrokers(t)
	topic := kafkaIntegrationTopic(t)

	testID := strings.ReplaceAll(uuid.NewString(), "-", "")
	groupID := "vsc-s2-integration-" + testID

	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("vsc-s2-integration-producer-"+testID),
	)
	if err != nil {
		t.Fatalf("create Kafka producer client: %v", err)
	}
	defer producerClient.Close()

	pingCtx, pingCancel := context.WithTimeout(
		ctx,
		10*time.Second,
	)
	defer pingCancel()

	if err := producerClient.Ping(pingCtx); err != nil {
		t.Fatalf("ping Kafka broker: %v", err)
	}

	publisher := NewPublisher(producerClient)

	marker := application.OutboundMessage{
		Topic: topic,
		Key:   []byte("marker-" + testID),
		Value: []byte("marker-value-" + testID),
		Headers: []application.MessageHeader{
			{
				Key:   "integration-test",
				Value: []byte(testID),
			},
		},
	}

	target := application.OutboundMessage{
		Topic: topic,
		Key:   []byte("target-" + testID),
		Value: []byte("target-value-" + testID),
		Headers: []application.MessageHeader{
			{
				Key:   "integration-test",
				Value: []byte(testID),
			},
		},
	}

	// 第一阶段：
	// marker 成功并提交；target 处理失败且不提交。
	handlerErr := errors.New("intentional integration handler failure")

	firstClient, firstAssigned := newIntegrationKafkaConsumer(
		t,
		brokers,
		topic,
		groupID,
		"vsc-s2-integration-first-"+testID,
		true,
	)
	closeFirstClient := closeKafkaClientOnce(firstClient)
	defer closeFirstClient()

	firstObserved := newObservedConsumer(firstClient)

	failedMessages := make(
		chan application.InboundMessage,
		1,
	)

	firstRunner := newConsumerRunner(
		firstObserved,
		application.MessageHandlerFunc(
			func(
				_ context.Context,
				message application.InboundMessage,
			) error {
				switch {
				case bytes.Equal(message.Key, marker.Key):
					return nil

				case bytes.Equal(message.Key, target.Key):
					failedMessages <- message
					return handlerErr

				default:
					return fmt.Errorf(
						"unexpected Kafka message key %q",
						message.Key,
					)
				}
			},
		),
	)

	firstCtx, firstCancel := context.WithCancel(ctx)
	defer firstCancel()

	firstDone := runKafkaConsumer(
		firstCtx,
		firstRunner,
	)

	waitForKafkaAssignment(
		t,
		ctx,
		firstAssigned,
		"first consumer",
	)

	if err := publisher.Publish(ctx, marker); err != nil {
		t.Fatalf("publish marker message: %v", err)
	}

	waitForKafkaCommit(
		t,
		ctx,
		firstObserved.commitSignal,
		"marker message",
	)

	if err := publisher.Publish(ctx, target); err != nil {
		t.Fatalf("publish target message: %v", err)
	}

	firstRunErr := waitForKafkaRunner(
		t,
		ctx,
		firstDone,
		"first consumer",
	)

	if !errors.Is(firstRunErr, handlerErr) {
		t.Fatalf(
			"first ConsumerRunner error = %v, want wrapped %v",
			firstRunErr,
			handlerErr,
		)
	}

	failedMessage := waitForKafkaMessage(
		t,
		ctx,
		failedMessages,
		"failed target message",
	)

	firstCommitted := firstObserved.committedRecords()
	if len(firstCommitted) != 1 {
		t.Fatalf(
			"first consumer committed %d records, want 1",
			len(firstCommitted),
		)
	}

	if !bytes.Equal(firstCommitted[0].Key, marker.Key) {
		t.Fatalf(
			"first committed key = %q, want marker key %q",
			firstCommitted[0].Key,
			marker.Key,
		)
	}

	firstCancel()
	closeFirstClient()

	// 第二阶段：
	// 使用相同 group 重启，必须重新收到未提交的 target。
	secondClient, secondAssigned := newIntegrationKafkaConsumer(
		t,
		brokers,
		topic,
		groupID,
		"vsc-s2-integration-second-"+testID,
		false,
	)
	closeSecondClient := closeKafkaClientOnce(secondClient)
	defer closeSecondClient()

	secondObserved := newObservedConsumer(secondClient)

	replayedMessages := make(
		chan application.InboundMessage,
		1,
	)

	secondRunner := newConsumerRunner(
		secondObserved,
		application.MessageHandlerFunc(
			func(
				_ context.Context,
				message application.InboundMessage,
			) error {
				if !bytes.Equal(message.Key, target.Key) {
					return fmt.Errorf(
						"unexpected replayed Kafka message key %q",
						message.Key,
					)
				}

				replayedMessages <- message
				return nil
			},
		),
	)

	secondCtx, secondCancel := context.WithCancel(ctx)
	defer secondCancel()

	secondDone := runKafkaConsumer(
		secondCtx,
		secondRunner,
	)

	waitForKafkaAssignment(
		t,
		ctx,
		secondAssigned,
		"second consumer",
	)

	replayedMessage := waitForKafkaMessage(
		t,
		ctx,
		replayedMessages,
		"replayed target message",
	)

	waitForKafkaCommit(
		t,
		ctx,
		secondObserved.commitSignal,
		"replayed target message",
	)

	secondCancel()

	secondRunErr := waitForKafkaRunner(
		t,
		ctx,
		secondDone,
		"second consumer",
	)
	if secondRunErr != nil {
		t.Fatalf(
			"second ConsumerRunner returned error: %v",
			secondRunErr,
		)
	}

	if replayedMessage.Topic != failedMessage.Topic ||
		replayedMessage.Partition != failedMessage.Partition ||
		replayedMessage.Offset != failedMessage.Offset {
		t.Fatalf(
			"replayed message position = %s/%d/%d, want %s/%d/%d",
			replayedMessage.Topic,
			replayedMessage.Partition,
			replayedMessage.Offset,
			failedMessage.Topic,
			failedMessage.Partition,
			failedMessage.Offset,
		)
	}

	if !bytes.Equal(replayedMessage.Key, target.Key) {
		t.Fatalf(
			"replayed message key = %q, want %q",
			replayedMessage.Key,
			target.Key,
		)
	}

	if !bytes.Equal(replayedMessage.Value, target.Value) {
		t.Fatalf(
			"replayed message value = %q, want %q",
			replayedMessage.Value,
			target.Value,
		)
	}

	secondCommitted := secondObserved.committedRecords()
	if len(secondCommitted) != 1 {
		t.Fatalf(
			"second consumer committed %d records, want 1",
			len(secondCommitted),
		)
	}

	if !bytes.Equal(secondCommitted[0].Key, target.Key) {
		t.Fatalf(
			"second committed key = %q, want target key %q",
			secondCommitted[0].Key,
			target.Key,
		)
	}

	closeSecondClient()

	// 第三阶段：
	// target 已成功提交，同一 group 再次启动时不得重放。
	thirdClient, thirdAssigned := newIntegrationKafkaConsumer(
		t,
		brokers,
		topic,
		groupID,
		"vsc-s2-integration-third-"+testID,
		false,
	)
	closeThirdClient := closeKafkaClientOnce(thirdClient)
	defer closeThirdClient()

	thirdObserved := newObservedConsumer(thirdClient)

	unexpectedReplayErr := errors.New(
		"committed Kafka record was replayed",
	)

	thirdRunner := newConsumerRunner(
		thirdObserved,
		application.MessageHandlerFunc(
			func(
				context.Context,
				application.InboundMessage,
			) error {
				return unexpectedReplayErr
			},
		),
	)

	thirdCtx, thirdCancel := context.WithCancel(ctx)
	defer thirdCancel()

	thirdDone := runKafkaConsumer(
		thirdCtx,
		thirdRunner,
	)

	waitForKafkaAssignment(
		t,
		ctx,
		thirdAssigned,
		"third consumer",
	)

	timer := time.AfterFunc(
		2*time.Second,
		thirdCancel,
	)
	defer timer.Stop()

	thirdRunErr := waitForKafkaRunner(
		t,
		ctx,
		thirdDone,
		"third consumer",
	)

	if errors.Is(thirdRunErr, unexpectedReplayErr) {
		t.Fatal("committed target message was replayed")
	}

	if thirdRunErr != nil {
		t.Fatalf(
			"third ConsumerRunner returned error: %v",
			thirdRunErr,
		)
	}

	if len(thirdObserved.committedRecords()) != 0 {
		t.Fatal("third consumer unexpectedly committed records")
	}
}

type observedConsumer struct {
	*kgo.Client

	mutex        sync.Mutex
	committed    []*kgo.Record
	commitSignal chan struct{}
}

func newObservedConsumer(
	client *kgo.Client,
) *observedConsumer {
	return &observedConsumer{
		Client:       client,
		commitSignal: make(chan struct{}, 8),
	}
}

func (consumer *observedConsumer) CommitRecords(
	ctx context.Context,
	records ...*kgo.Record,
) error {
	if err := consumer.Client.CommitRecords(
		ctx,
		records...,
	); err != nil {
		return err
	}

	consumer.mutex.Lock()
	consumer.committed = append(
		consumer.committed,
		records...,
	)
	consumer.mutex.Unlock()

	select {
	case consumer.commitSignal <- struct{}{}:
	default:
	}

	return nil
}

func (
	consumer *observedConsumer,
) committedRecords() []*kgo.Record {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()

	result := make(
		[]*kgo.Record,
		len(consumer.committed),
	)
	copy(result, consumer.committed)

	return result
}

func newIntegrationKafkaConsumer(
	t *testing.T,
	brokers []string,
	topic string,
	groupID string,
	clientID string,
	startAtEnd bool,
) (*kgo.Client, <-chan struct{}) {
	t.Helper()

	assigned := make(chan struct{})
	var assignedOnce sync.Once

	resetOffset := kgo.NewOffset().AtStart()
	if startAtEnd {
		resetOffset = kgo.NewOffset().AtEnd()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(resetOffset),
		kgo.OnPartitionsAssigned(
			func(
				_ context.Context,
				_ *kgo.Client,
				partitions map[string][]int32,
			) {
				if len(partitions[topic]) == 0 {
					return
				}

				assignedOnce.Do(func() {
					close(assigned)
				})
			},
		),
	)
	if err != nil {
		t.Fatalf("create Kafka consumer client: %v", err)
	}

	return client, assigned
}

func runKafkaConsumer(
	ctx context.Context,
	runner *ConsumerRunner,
) <-chan error {
	done := make(chan error, 1)

	go func() {
		done <- runner.Run(ctx)
	}()

	return done
}

func waitForKafkaAssignment(
	t *testing.T,
	ctx context.Context,
	assigned <-chan struct{},
	name string,
) {
	t.Helper()

	select {
	case <-assigned:
	case <-ctx.Done():
		t.Fatalf(
			"wait for %s assignment: %v",
			name,
			ctx.Err(),
		)
	}
}

func waitForKafkaCommit(
	t *testing.T,
	ctx context.Context,
	signal <-chan struct{},
	name string,
) {
	t.Helper()

	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf(
			"wait for %s commit: %v",
			name,
			ctx.Err(),
		)
	}
}

func waitForKafkaRunner(
	t *testing.T,
	ctx context.Context,
	done <-chan error,
	name string,
) error {
	t.Helper()

	select {
	case err := <-done:
		return err

	case <-ctx.Done():
		t.Fatalf(
			"wait for %s completion: %v",
			name,
			ctx.Err(),
		)
		return nil
	}
}

func waitForKafkaMessage(
	t *testing.T,
	ctx context.Context,
	messages <-chan application.InboundMessage,
	name string,
) application.InboundMessage {
	t.Helper()

	select {
	case message := <-messages:
		return message

	case <-ctx.Done():
		t.Fatalf(
			"wait for %s: %v",
			name,
			ctx.Err(),
		)
		return application.InboundMessage{}
	}
}

func closeKafkaClientOnce(
	client *kgo.Client,
) func() {
	var once sync.Once

	return func() {
		once.Do(client.CloseAllowingRebalance)
	}
}

func kafkaIntegrationBrokers(
	t *testing.T,
) []string {
	t.Helper()

	raw := strings.TrimSpace(
		os.Getenv("TEST_KAFKA_BROKERS"),
	)
	if raw == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set")
	}

	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	if len(brokers) == 0 {
		t.Fatal("TEST_KAFKA_BROKERS contains no broker addresses")
	}

	return brokers
}

func kafkaIntegrationTopic(
	t *testing.T,
) string {
	t.Helper()

	topic := strings.TrimSpace(
		os.Getenv("TEST_KAFKA_TOPIC"),
	)
	if topic == "" {
		t.Skip("TEST_KAFKA_TOPIC is not set")
	}

	return topic
}
