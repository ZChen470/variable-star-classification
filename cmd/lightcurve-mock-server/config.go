package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/ZChen470/variable-star-classification/internal/project"
)

const (
	envLightCurveMockDataDir    = "LIGHTCURVE_MOCK_DATA_DIR"
	envLightCurveMockListenAddr = "LIGHTCURVE_MOCK_LISTEN_ADDR"

	envKafkaBrokers   = "KAFKA_BROKERS"
	envKafkaClientID  = "KAFKA_CLIENT_ID"
	envCandidateTopic = "CANDIDATE_TOPIC"

	envCandidateRatePerSecond = "CANDIDATE_RATE_PER_SECOND"

	defaultLightCurveMockListenAddr = "127.0.0.1:18081"
	defaultCandidateRatePerSecond   = 1.0

	defaultKafkaClientID = project.Name + "-lightcurve-mock-server"
)

type lightCurveMockConfig struct {
	dataDir    string
	listenAddr string

	kafkaBrokers   []string
	kafkaClientID  string
	candidateTopic string

	candidateRatePerSecond float64
}

func loadLightCurveMockConfig(
	lookup func(string) (string, bool),
) (lightCurveMockConfig, error) {
	if lookup == nil {
		return lightCurveMockConfig{},
			errors.New(
				"environment lookup must not be nil",
			)
	}

	dataDir, err :=
		requiredTrimmedEnvironmentValue(
			lookup,
			envLightCurveMockDataDir,
		)
	if err != nil {
		return lightCurveMockConfig{}, err
	}

	rawBrokers, err :=
		requiredTrimmedEnvironmentValue(
			lookup,
			envKafkaBrokers,
		)
	if err != nil {
		return lightCurveMockConfig{}, err
	}

	kafkaBrokers, err :=
		parseKafkaBrokers(rawBrokers)
	if err != nil {
		return lightCurveMockConfig{}, err
	}

	candidateTopic, err :=
		requiredTrimmedEnvironmentValue(
			lookup,
			envCandidateTopic,
		)
	if err != nil {
		return lightCurveMockConfig{}, err
	}

	listenAddr :=
		defaultLightCurveMockListenAddr

	if raw, ok :=
		lookup(envLightCurveMockListenAddr); ok {
		trimmed :=
			strings.TrimSpace(raw)

		if trimmed != "" {
			listenAddr = trimmed
		}
	}

	kafkaClientID :=
		defaultKafkaClientID

	if raw, ok := lookup(envKafkaClientID); ok {
		trimmed :=
			strings.TrimSpace(raw)

		if trimmed != "" {
			kafkaClientID = trimmed
		}
	}

	candidateRatePerSecond :=
		defaultCandidateRatePerSecond

	if raw, ok :=
		lookup(envCandidateRatePerSecond); ok {
		trimmed :=
			strings.TrimSpace(raw)

		if trimmed != "" {
			rate, parseErr :=
				strconv.ParseFloat(
					trimmed,
					64,
				)
			if parseErr != nil {
				return lightCurveMockConfig{},
					fmt.Errorf(
						"environment variable %s must be a positive finite number: %w",
						envCandidateRatePerSecond,
						parseErr,
					)
			}

			if rate <= 0 ||
				math.IsNaN(rate) ||
				math.IsInf(rate, 0) {
				return lightCurveMockConfig{},
					fmt.Errorf(
						"environment variable %s must be a positive finite number",
						envCandidateRatePerSecond,
					)
			}

			candidateRatePerSecond =
				rate
		}
	}

	return lightCurveMockConfig{
		dataDir: dataDir,

		listenAddr: listenAddr,

		kafkaBrokers: kafkaBrokers,

		kafkaClientID: kafkaClientID,

		candidateTopic: candidateTopic,

		candidateRatePerSecond: candidateRatePerSecond,
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

	trimmed :=
		strings.TrimSpace(value)

	if trimmed == "" {
		return "", fmt.Errorf(
			"environment variable %s must be set and non-blank",
			name,
		)
	}

	return trimmed, nil
}

func parseKafkaBrokers(
	raw string,
) ([]string, error) {
	parts :=
		strings.Split(
			raw,
			",",
		)

	brokers := make(
		[]string,
		0,
		len(parts),
	)

	for index, part := range parts {
		broker :=
			strings.TrimSpace(part)

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
