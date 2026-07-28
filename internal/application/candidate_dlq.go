package application

import (
	"errors"
	"strconv"
)

const (
	CandidateDLQHeaderErrorCode         = "x-astro-error-code"
	CandidateDLQHeaderOriginalTopic     = "x-astro-original-topic"
	CandidateDLQHeaderOriginalPartition = "x-astro-original-partition"
	CandidateDLQHeaderOriginalOffset    = "x-astro-original-offset"
)

// BuildCandidateDLQMessage 构造尚未发布的 Candidate DLQ 消息
//
// 原始 Key、Value、Headers 和 Kafka Record timestamp 均被保留：
// 错误元数据 Headers 追加在原始 Headers 之后
func BuildCandidateDLQMessage(dlqTopic string, message InboundMessage, candidateErr *PermanentCandidateMessageError) (OutboundMessage, error) {
	if dlqTopic == "" {
		return OutboundMessage{}, errors.New("candidate DLQ topic must not be empty")
	}

	if candidateErr == nil {
		return OutboundMessage{}, errors.New("permanent candidate message error must not be nil")
	}

	if candidateErr.Code == "" {
		return OutboundMessage{}, errors.New("permanent candidate message error code must not be empty")
	}

	headers := cloneCandidateDLQHeaders(message.Headers)
	headers = append(headers, MessageHeader{
		Key:   CandidateDLQHeaderErrorCode,
		Value: []byte(candidateErr.Code),
	},
		MessageHeader{
			Key:   CandidateDLQHeaderOriginalTopic,
			Value: []byte(message.Topic),
		},
		MessageHeader{
			Key:   CandidateDLQHeaderOriginalPartition,
			Value: []byte(strconv.FormatInt(int64(message.Partition), 10)),
		},
		MessageHeader{
			Key:   CandidateDLQHeaderOriginalOffset,
			Value: []byte(strconv.FormatInt(int64(message.Offset), 10)),
		},
	)

	return OutboundMessage{
		Topic:     dlqTopic,
		Key:       cloneCandidateDLQBytes(message.Key),
		Value:     cloneCandidateDLQBytes(message.Value),
		Headers:   headers,
		Timestamp: message.Timestamp,
	}, nil
}

func cloneCandidateDLQHeaders(headers []MessageHeader) []MessageHeader {
	if len(headers) == 0 {
		return nil
	}

	cloned := make([]MessageHeader, len(headers))
	for index, header := range headers {
		cloned[index].Key = header.Key
		cloned[index].Value = cloneCandidateDLQBytes(header.Value)
	}

	return cloned
}

func cloneCandidateDLQBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
