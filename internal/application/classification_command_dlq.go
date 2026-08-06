package application

import (
	"errors"
	"strconv"
)

const (
	ClassificationCommandDLQHeaderErrorCode = "x-astro-error-code"

	ClassificationCommandDLQHeaderErrorClass = "x-astro-error-class"

	ClassificationCommandDLQHeaderErrorOperation = "x-astro-error-operation"

	ClassificationCommandDLQHeaderOriginalTopic = "x-astro-original-topic"

	ClassificationCommandDLQHeaderOriginalPartition = "x-astro-original-partition"

	ClassificationCommandDLQHeaderOriginalOffset = "x-astro-original-offset"
)

// BuildClassificationCommandDLQMessage 构造尚未发布的 Command DLQ 消息。
//
// 原始 Key、Value、Headers 和 Kafka Record Timestamp 均被保留。
// 稳定错误元数据追加在原始 Headers 之后。
// 不写入 Cause.Error()，避免将不稳定错误文本作为 DLQ 契约。
func BuildClassificationCommandDLQMessage(
	dlqTopic string,
	message InboundMessage,
	workerError *ClassificationWorkerError,
) (OutboundMessage, error) {
	if dlqTopic == "" {
		return OutboundMessage{}, errors.New(
			"classification command DLQ topic must not be empty",
		)
	}

	if workerError == nil {
		return OutboundMessage{}, errors.New(
			"classification worker error must not be nil",
		)
	}

	if workerError.Class !=
		ClassificationWorkerErrorClassPermanent {
		return OutboundMessage{}, errors.New(
			"classification command DLQ requires a permanent worker error",
		)
	}

	if workerError.Code == "" {
		return OutboundMessage{}, errors.New(
			"classification worker error code must not be empty",
		)
	}

	if workerError.Operation == "" {
		return OutboundMessage{}, errors.New(
			"classification worker error operation must not be empty",
		)
	}

	headers := cloneClassificationCommandDLQHeaders(
		message.Headers,
	)

	headers = append(
		headers,
		MessageHeader{
			Key: ClassificationCommandDLQHeaderErrorCode,
			Value: []byte(
				workerError.Code,
			),
		},
		MessageHeader{
			Key: ClassificationCommandDLQHeaderErrorClass,
			Value: []byte(
				workerError.Class.String(),
			),
		},
		MessageHeader{
			Key: ClassificationCommandDLQHeaderErrorOperation,
			Value: []byte(
				workerError.Operation,
			),
		},
		MessageHeader{
			Key: ClassificationCommandDLQHeaderOriginalTopic,
			Value: []byte(
				message.Topic,
			),
		},
		MessageHeader{
			Key: ClassificationCommandDLQHeaderOriginalPartition,
			Value: []byte(
				strconv.FormatInt(
					int64(message.Partition),
					10,
				),
			),
		},
		MessageHeader{
			Key: ClassificationCommandDLQHeaderOriginalOffset,
			Value: []byte(
				strconv.FormatInt(
					message.Offset,
					10,
				),
			),
		},
	)

	return OutboundMessage{
		Topic: dlqTopic,

		Key: cloneClassificationCommandDLQBytes(
			message.Key,
		),

		Value: cloneClassificationCommandDLQBytes(
			message.Value,
		),

		Headers: headers,

		Timestamp: message.Timestamp,
	}, nil
}

func cloneClassificationCommandDLQHeaders(
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
		cloned[index].Key = header.Key

		cloned[index].Value =
			cloneClassificationCommandDLQBytes(
				header.Value,
			)
	}

	return cloned
}

func cloneClassificationCommandDLQBytes(
	value []byte,
) []byte {
	if value == nil {
		return nil
	}

	cloned := make([]byte, len(value))
	copy(cloned, value)

	return cloned
}
