package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	envKafkaBrokers         = "KAFKA_BROKERS"
	envKafkaConsumerGroup   = "KAFKA_CONSUMER_GROUP"
	envKafkaTopic           = "KAFKA_TOPIC"
	envKafkaSASLUsername    = "KAFKA_SASL_USERNAME"
	envKafkaSASLPassword    = "KAFKA_SASL_PASSWORD"
	envKafkaClientID        = "KAFKA_CLIENT_ID"
	envRefreshInterval      = "BACKLOG_REFRESH_INTERVAL"
	envRefreshTimeout       = "BACKLOG_REFRESH_TIMEOUT"
	envManagementListenAddr = "MANAGEMENT_LISTEN_ADDR"

	defaultKafkaClientID        = "kafka-backlog-exporter"
	defaultRefreshInterval      = 15 * time.Second
	defaultRefreshTimeout       = 10 * time.Second
	defaultManagementListenAddr = ":9091"
)

type config struct {
	kafkaBrokers       []string
	kafkaConsumerGroup string
	kafkaTopic         string
	kafkaSASLUsername  string
	kafkaSASLPassword  string
	kafkaClientID      string

	refreshInterval time.Duration
	refreshTimeout  time.Duration

	managementListenAddr string
}

func loadConfig() (config, error) {
	brokers, err := parseBrokers(os.Getenv(envKafkaBrokers))
	if err != nil {
		return config{}, err
	}

	group := strings.TrimSpace(
		os.Getenv(envKafkaConsumerGroup),
	)
	if group == "" {
		return config{}, fmt.Errorf(
			"%s must not be blank",
			envKafkaConsumerGroup,
		)
	}

	topic := strings.TrimSpace(os.Getenv(envKafkaTopic))
	if topic == "" {
		return config{}, fmt.Errorf(
			"%s must not be blank",
			envKafkaTopic,
		)
	}

	username := strings.TrimSpace(
		os.Getenv(envKafkaSASLUsername),
	)
	password := os.Getenv(envKafkaSASLPassword)

	if (username == "") != (password == "") {
		return config{}, errors.New(
			"KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be configured together",
		)
	}

	clientID := strings.TrimSpace(
		os.Getenv(envKafkaClientID),
	)
	if clientID == "" {
		clientID = defaultKafkaClientID
	}

	refreshInterval, err := parsePositiveDuration(
		envRefreshInterval,
		os.Getenv(envRefreshInterval),
		defaultRefreshInterval,
	)
	if err != nil {
		return config{}, err
	}

	refreshTimeout, err := parsePositiveDuration(
		envRefreshTimeout,
		os.Getenv(envRefreshTimeout),
		defaultRefreshTimeout,
	)
	if err != nil {
		return config{}, err
	}

	managementListenAddr := strings.TrimSpace(
		os.Getenv(envManagementListenAddr),
	)
	if managementListenAddr == "" {
		managementListenAddr = defaultManagementListenAddr
	}

	return config{
		kafkaBrokers:         brokers,
		kafkaConsumerGroup:   group,
		kafkaTopic:           topic,
		kafkaSASLUsername:    username,
		kafkaSASLPassword:    password,
		kafkaClientID:        clientID,
		refreshInterval:      refreshInterval,
		refreshTimeout:       refreshTimeout,
		managementListenAddr: managementListenAddr,
	}, nil
}

func parseBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		brokers = append(brokers, part)
	}

	if len(brokers) == 0 {
		return nil, fmt.Errorf(
			"%s must contain at least one broker",
			envKafkaBrokers,
		)
	}

	return brokers, nil
}

func parsePositiveDuration(
	name string,
	raw string,
	fallback time.Duration,
) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf(
			"parse %s: %w",
			name,
			err,
		)
	}
	if value <= 0 {
		return 0, fmt.Errorf(
			"%s must be positive",
			name,
		)
	}

	return value, nil
}
