package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ZChen470/variable-star-classification/internal/project"
)

const (
	envKafkaBrokers                = "KAFKA_BROKERS"
	envKafkaConsumerGroup          = "KAFKA_CONSUMER_GROUP"
	envKafkaClientID               = "KAFKA_CLIENT_ID"
	envCandidateTopic              = "CANDIDATE_TOPIC"
	envClassificationCommandTopic  = "CLASSIFICATION_COMMAND_TOPIC"
	envCandidateDLQTopic           = "CANDIDATE_DLQ_TOPIC"
	envModelBundleVersion          = "MODEL_BUNDLE_VERSION"
	envClassificationPolicyVersion = "CLASSIFICATION_POLICY_VERSION"
	defaultKafkaClientID           = project.Name + "-candidate-orchestrator"
)

type candidateOrchestratorConfig struct {
	kafkaBrokers                []string
	kafkaConsumerGroup          string
	kafkaClientID               string
	candidateTopic              string
	classificationCommandTopic  string
	candidateDLQTopic           string
	modelBundleVersion          string
	classificationPolicyVersion string
}

func loadCandidateOrchestratorConfig(lookup func(string) (string, bool)) (candidateOrchestratorConfig, error) {
	if lookup == nil {
		return candidateOrchestratorConfig{},
			errors.New("environment lookup must not be nil")
	}

	rawBrokers, err := requiredExactEnvironmentValue(
		lookup,
		envKafkaBrokers,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	brokers, err := parseKafkaBrokers(rawBrokers)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	consumerGroup, err := requiredTrimmedEnvironmentValue(
		lookup,
		envKafkaConsumerGroup,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	candidateTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envCandidateTopic,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	commandTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envClassificationCommandTopic,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	dlqTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envCandidateDLQTopic,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	modelBundleVersion, err := requiredExactEnvironmentValue(
		lookup,
		envModelBundleVersion,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	classificationPolicyVersion, err := requiredExactEnvironmentValue(
		lookup,
		envClassificationPolicyVersion,
	)
	if err != nil {
		return candidateOrchestratorConfig{}, err
	}

	clientID := defaultKafkaClientID
	if rawClientID, ok := lookup(envKafkaClientID); ok {
		trimmedClientID := strings.TrimSpace(rawClientID)
		if trimmedClientID != "" {
			clientID = trimmedClientID
		}
	}

	return candidateOrchestratorConfig{
		kafkaBrokers:                brokers,
		kafkaConsumerGroup:          consumerGroup,
		kafkaClientID:               clientID,
		candidateTopic:              candidateTopic,
		classificationCommandTopic:  commandTopic,
		candidateDLQTopic:           dlqTopic,
		modelBundleVersion:          modelBundleVersion,
		classificationPolicyVersion: classificationPolicyVersion,
	}, nil
}

func requiredTrimmedEnvironmentValue(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok {
		return "", fmt.Errorf(
			"environment variable %s must be set and non-blank",
			name,
		)
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf(
			"environment variable %s must be set and non-blank",
			name,
		)
	}

	return trimmed, nil
}

func requiredExactEnvironmentValue(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf(
			"environment variable %s must be set and non-blank",
			name,
		)
	}

	return value, nil
}

func parseKafkaBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))

	for index, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf(
				"%s contains empty broker at position %d",
				envKafkaBrokers,
				index+1,
			)
		}

		brokers = append(brokers, broker)
	}

	return brokers, nil
}
