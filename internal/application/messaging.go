package application

import (
	"context"
	"errors"
	"time"
)

// MessageHeader 是与具体 Kafka 客户端类型无关的消息头
type MessageHeader struct {
	Key   string
	Value []byte
}

// OutboundMessage 是应用层要求发布的一条消息
type OutboundMessage struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
	Timestamp time.Time
}

// MessagePublisher 是应用层使用的消息发布 Port
type MessagePublisher interface {
	Publish(ctx context.Context, message OutboundMessage) error
}

// InboundMessage 是应用层接收到的一条消息
type InboundMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
	Timestamp time.Time
}

// MessageHandler 处理一条入站消息
//
// 返回 nil 后，Kafka Adapter 才能提交对应 offset
type MessageHandler interface {
	Handle(ctx context.Context, message InboundMessage) error
}

// MessageHandlerFunc 允许普通函数作为 MessageHandler 使用
type MessageHandlerFunc func(ctx context.Context, message InboundMessage) error

// Handle 实现 MessageHandler
func (handler MessageHandlerFunc) Handle(ctx context.Context, message InboundMessage) error {
	if handler == nil {
		return errors.New("nil message handler function")
	}

	return handler(ctx, message)
}
