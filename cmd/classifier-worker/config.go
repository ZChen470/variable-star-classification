package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ZChen470/variable-star-classification/internal/project"
)

const (
	envKafkaBrokers       = "KAFKA_BROKERS"
	envKafkaConsumerGroup = "KAFKA_CONSUMER_GROUP"
	envKafkaClientID      = "KAFKA_CLIENT_ID"

	envClassificationCommandTopic = "CLASSIFICATION_COMMAND_TOPIC"
	envClassificationResultTopic  = "CLASSIFICATION_RESULT_TOPIC"

	envClassificationCommandDLQTopic = "CLASSIFICATION_COMMAND_DLQ_TOPIC"

	envModelBundleVersion      = "MODEL_BUNDLE_VERSION"
	envModelBundleManifestPath = "MODEL_BUNDLE_MANIFEST_PATH"

	envTritonBaseURL = "TRITON_BASE_URL"
	envPostgresDSN   = "POSTGRES_DSN"

	envLightCurveBaseURL    = "LIGHT_CURVE_BASE_URL"
	envManagementListenAddr = "MANAGEMENT_LISTEN_ADDR"

	envKafkaSASLUsername = "KAFKA_SASL_USERNAME"
	envKafkaSASLPassword = "KAFKA_SASL_PASSWORD"

	defaultManagementListenAddr = "127.0.0.1:9091"

	defaultKafkaClientID = project.Name + "-classifier-worker"
)

type classifierWorkerConfig struct {
	kafkaBrokers       []string
	kafkaConsumerGroup string
	kafkaClientID      string

	classificationCommandTopic    string
	classificationResultTopic     string
	classificationCommandDLQTopic string

	modelBundleVersion      string
	modelBundleManifestPath string

	tritonBaseURL string
	postgresDSN   string

	lightCurveBaseURL    string
	managementListenAddr string

	kafkaSASLUsername string
	kafkaSASLPassword string
}

func loadClassifierWorkerConfig(
	lookup func(string) (string, bool),
) (classifierWorkerConfig, error) {
	if lookup == nil {
		return classifierWorkerConfig{},
			errors.New("environment lookup must not be nil")
	}

	rawBrokers, err := requiredExactEnvironmentValue(
		lookup,
		envKafkaBrokers,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	brokers, err := parseKafkaBrokers(rawBrokers)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	kafkaSASLUsername, kafkaSASLPassword, err := loadKafkaSASLCredentials(lookup)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	consumerGroup, err := requiredTrimmedEnvironmentValue(
		lookup,
		envKafkaConsumerGroup,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	commandTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envClassificationCommandTopic,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	resultTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envClassificationResultTopic,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	commandDLQTopic, err := requiredTrimmedEnvironmentValue(
		lookup,
		envClassificationCommandDLQTopic,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	modelBundleVersion, err := requiredExactEnvironmentValue(
		lookup,
		envModelBundleVersion,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	manifestPath, err := requiredExactEnvironmentValue(
		lookup,
		envModelBundleManifestPath,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	tritonBaseURL, err := requiredExactEnvironmentValue(
		lookup,
		envTritonBaseURL,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	postgresDSN, err := requiredExactEnvironmentValue(
		lookup,
		envPostgresDSN,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	lightCurveBaseURL, err := requiredExactEnvironmentValue(
		lookup,
		envLightCurveBaseURL,
	)
	if err != nil {
		return classifierWorkerConfig{}, err
	}

	clientID := defaultKafkaClientID

	if rawClientID, ok := lookup(envKafkaClientID); ok {
		trimmedClientID := strings.TrimSpace(rawClientID)
		if trimmedClientID != "" {
			clientID = trimmedClientID
		}
	}

	managementListenAddr := defaultManagementListenAddr

	if rawAddr, ok := lookup(envManagementListenAddr); ok {
		trimmedAddr := strings.TrimSpace(rawAddr)
		if trimmedAddr != "" {
			managementListenAddr = trimmedAddr
		}
	}

	return classifierWorkerConfig{
		kafkaBrokers:       brokers,
		kafkaConsumerGroup: consumerGroup,
		kafkaClientID:      clientID,

		classificationCommandTopic:    commandTopic,
		classificationResultTopic:     resultTopic,
		classificationCommandDLQTopic: commandDLQTopic,

		modelBundleVersion:      modelBundleVersion,
		modelBundleManifestPath: manifestPath,

		tritonBaseURL: tritonBaseURL,
		postgresDSN:   postgresDSN,

		lightCurveBaseURL:    lightCurveBaseURL,
		managementListenAddr: managementListenAddr,

		kafkaSASLUsername: kafkaSASLUsername,
		kafkaSASLPassword: kafkaSASLPassword,
	}, nil
}

func requiredTrimmedEnvironmentValue(
	lookup func(string) (string, bool),
	name string,
) (string, error) {
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

func requiredExactEnvironmentValue(
	lookup func(string) (string, bool),
	name string,
) (string, error) {
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf(
			"environment variable %s must be set and non-blank",
			name,
		)
	}

	return value, nil
}

func parseKafkaBrokers(
	raw string,
) ([]string, error) {
	parts := strings.Split(raw, ",")

	brokers := make(
		[]string,
		0,
		len(parts),
	)

	for index, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf(
				"%s contains empty broker at position %d",
				envKafkaBrokers,
				index+1,
			)
		}

		brokers = append(
			brokers,
			broker,
		)
	}

	return brokers, nil
}

func loadKafkaSASLCredentials(lookup func(string) (string, bool)) (string, string, error) {
	rawUsername, usernameSet := lookup(envKafkaSASLUsername)
	rawPassword, passwordSet := lookup(envKafkaSASLPassword)

	if !usernameSet && !passwordSet {
		return "", "", nil
	}

	username := strings.TrimSpace(rawUsername)
	if !usernameSet || username == "" {
		return "", "", fmt.Errorf(
			"environment variable %s must be set and non-blank when Kafka SASL is configured",
			envKafkaSASLUsername,
		)
	}

	if !passwordSet || strings.TrimSpace(rawPassword) == "" {
		return "", "", fmt.Errorf(
			"environment variable %s must be set and non-blank when Kafka SASL is configured",
			envKafkaSASLPassword,
		)
	}

	return username, rawPassword, nil
}
