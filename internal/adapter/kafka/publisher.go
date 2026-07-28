package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/application"

	"github.com/twmb/franz-go/pkg/kgo"
)

type syncProducer interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
}

// Publisher 使用 franz-go 同步发布消息
//
// Publisher 只有在 Kafka 返回生产结果后才返回
type Publisher struct {
	producer syncProducer
}

var _ application.MessagePublisher = (*Publisher)(nil)

func NewPublisher(client *kgo.Client) *Publisher {
	if client == nil {
		return &Publisher{}
	}
	// franze-go Client 作为 syncProducer（同步模式生产者）的实现
	return &Publisher{
		producer: client,
	}
}

func newPublisher(producer syncProducer) *Publisher {
	return &Publisher{
		producer: producer,
	}
}

// Publish 被应用层调用，将应用层消息转换成 Kafka Record 并同步等待生产结果
func (publisher *Publisher) Publish(ctx context.Context, message application.OutboundMessage) error {
	// 1.校验，处理错误
	if ctx == nil {
		return errors.New("publish kafka message: nil context")
	}

	if publisher == nil || publisher.producer == nil {
		return errors.New("publish Kafka message: nil producer")
	}

	if err := validateOutboundMessage(message); err != nil {
		return fmt.Errorf("publish Kafka message: %w", err)
	}

	// 2.构建消息
	record := &kgo.Record{
		Key:       cloneBytes(message.Key),
		Value:     cloneBytes(message.Value),
		Timestamp: message.Timestamp,
		Topic:     message.Topic,
	}

	if len(message.Headers) > 0 {
		record.Headers = make([]kgo.RecordHeader, 0, len(message.Headers))

		for _, header := range message.Headers {
			record.Headers = append(
				record.Headers,
				kgo.RecordHeader{
					Key:   header.Key,
					Value: cloneBytes(header.Value),
				},
			)
		}
	}

	results := publisher.producer.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka record to topic %q: %w", message.Topic, err)
	}

	return nil
}

func validateOutboundMessage(message application.OutboundMessage) error {
	if message.Topic == "" {
		return errors.New("topic is empty")
	}

	// 本项目所有业务 Topic 都以 object_id 为 key
	//if len(message.Key) == 0 {
	//	return errors.New("message key is empty")
	//}
	//
	//if len(message.Value) == 0 {
	//	return errors.New("message value is empty")
	//}

	// 允许空 header key
	//for index, header := range message.Headers {
	//	if header.Key == "" {
	//		return fmt.Errorf("header %d key is empty", index)
	//	}
	//}

	return nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)

	return cloned
}
