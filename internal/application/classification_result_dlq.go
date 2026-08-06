package application

import (
	"errors"
	"strconv"
)

const (
	ClassificationResultDLQHeaderErrorCode = "x-astro-error-code"

	ClassificationResultDLQHeaderErrorClass = "x-astro-error-class"

	ClassificationResultDLQHeaderErrorField = "x-astro-error-field"

	ClassificationResultDLQHeaderOriginalTopic = "x-astro-original-topic"

	ClassificationResultDLQHeaderOriginalPartition = "x-astro-original-partition"

	ClassificationResultDLQHeaderOriginalOffset = "x-astro-original-offset"
)

// BuildClassificationResultDLQMessage 构造尚未发布的 Result DLQ 消息。
//
// 原始 Key、Value、Headers 和 Kafka Timestamp 均被保留。
// 稳定错误元数据追加到原始 Headers 之后。
// Cause.Error() 不进入 Header，避免把不稳定错误文本变成消息契约。
func BuildClassificationResultDLQMessage(
	dlqTopic string,
	message InboundMessage,
	resultError *PermanentClassificationResultError,
) (OutboundMessage, error) {
	if dlqTopic == "" {
		return OutboundMessage{}, errors.New(
			"classification result DLQ topic must not be empty",
		)
	}

	if resultError == nil {
		return OutboundMessage{}, errors.New(
			"permanent classification result error must not be nil",
		)
	}

	if resultError.Code == "" {
		return OutboundMessage{}, errors.New(
			"classification result error code must not be empty",
		)
	}

	headers := cloneClassificationResultDLQHeaders(
		message.Headers,
	)

	headers = append(
		headers,
		MessageHeader{
			Key: ClassificationResultDLQHeaderErrorCode,
			Value: []byte(
				resultError.Code,
			),
		},
		MessageHeader{
			Key: ClassificationResultDLQHeaderErrorClass,
			Value: []byte(
				"PERMANENT",
			),
		},
		MessageHeader{
			Key: ClassificationResultDLQHeaderErrorField,
			Value: []byte(
				resultError.Field,
			),
		},
		MessageHeader{
			Key: ClassificationResultDLQHeaderOriginalTopic,
			Value: []byte(
				message.Topic,
			),
		},
		MessageHeader{
			Key: ClassificationResultDLQHeaderOriginalPartition,
			Value: []byte(
				strconv.FormatInt(
					int64(message.Partition),
					10,
				),
			),
		},
		MessageHeader{
			Key: ClassificationResultDLQHeaderOriginalOffset,
			Value: []byte(
				strconv.FormatInt(
					int64(message.Offset),
					10,
				),
			),
		},
	)

	return OutboundMessage{
		Topic: dlqTopic,

		Key: cloneClassificationResultDLQBytes(
			message.Key,
		),

		Value: cloneClassificationResultDLQBytes(
			message.Value,
		),

		Headers: headers,

		Timestamp: message.Timestamp,
	}, nil
}

func cloneClassificationResultDLQHeaders(
	headers []MessageHeader,
) []MessageHeader {
	if headers == nil {
		return nil
	}

	cloned := make(
		[]MessageHeader,
		len(headers),
	)

	for index, header := range headers {
		cloned[index] = MessageHeader{
			Key: header.Key,

			Value: cloneClassificationResultDLQBytes(
				header.Value,
			),
		}
	}

	return cloned
}

func cloneClassificationResultDLQBytes(
	value []byte,
) []byte {
	if value == nil {
		return nil
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)

	return cloned
}
