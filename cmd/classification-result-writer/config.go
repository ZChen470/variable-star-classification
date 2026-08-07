package main

import (
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/project"
	"strings"
)

const (
	envKafkaBrokers       = "KAFKA_BROKERS"
	envKafkaConsumerGroup = "KAFKA_CONSUMER_GROUP"
	envKafkaClientID      = "KAFKA_CLIENT_ID"

	envClassificationResultTopic    = "CLASSIFICATION_RESULT_TOPIC"
	envClassificationResultDLQTopic = "CLASSIFICATION_RESULT_DLQ_TOPIC"

	envPostgresDSN = "POSTGRES_DSN"

	defaultKafkaClientID = project.Name + "-classification-result-writer"
)

type classificationResultWriterConfig struct {
	kafkaBrokers       []string
	kafkaConsumerGroup string
	kafkaClientID      string

	classificationResultTopic    string
	classificationResultDLQTopic string

	postgresDSN string
}

func loadClassificationResultWriterConfig(lookup func(string) (string, bool)) (classificationResultWriterConfig, error) {
	if lookup == nil {
		return classificationResultWriterConfig{}, errors.New("environment lookup must not be nil")
	}

	rawBrokers, err := requiredExactEnvironmentValue(lookup, envKafkaBrokers)
	if err != nil {
		return classificationResultWriterConfig{}, err
	}
	brokers, err := parseKafkaBrokers(rawBrokers)
	if err != nil {
		return classificationResultWriterConfig{}, err
	}

	consumerGroup, err := requiredTrimmedEnvironmentValue(
		lookup,
		envKafkaConsumerGroup,
	)
	if err != nil {
		return classificationResultWriterConfig{}, err
	}

	resultTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envClassificationResultTopic,
	)
	if err != nil {
		return classificationResultWriterConfig{}, err
	}

	resultDLQTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envClassificationResultDLQTopic,
	)
	if err != nil {
		return classificationResultWriterConfig{}, err
	}

	postgresDSN, err := requiredExactEnvironmentValue(
		lookup,
		envPostgresDSN,
	)
	if err != nil {
		return classificationResultWriterConfig{}, err
	}

	clientID := defaultKafkaClientID

	if rawClientID, ok := lookup(envKafkaClientID); ok {
		trimmedClientID := strings.TrimSpace(rawClientID)
		if trimmedClientID != "" {
			clientID = trimmedClientID
		}
	}

	return classificationResultWriterConfig{
		kafkaBrokers:                 brokers,
		kafkaConsumerGroup:           consumerGroup,
		kafkaClientID:                clientID,
		classificationResultTopic:    resultTopic,
		classificationResultDLQTopic: resultDLQTopic,
		postgresDSN:                  postgresDSN,
	}, nil
}

func requiredExactEnvironmentValue(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("environment variable %s must be set and non-blank", name)
	}
	return value, nil
}

func parseKafkaBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")

	brokers := make([]string, 0, len(parts))

	for index, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf("%s contains empty broker at position %d", envKafkaBrokers, index+1)
		}
		brokers = append(brokers, broker)
	}
	return brokers, nil
}

func requiredTrimmedEnvironmentValue(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok {
		return "", fmt.Errorf("environment variable %s must be set and non-blanck", name)
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("environment variable %s must be set and non-blank", name)
	}

	return trimmed, nil
}
