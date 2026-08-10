package main

import (
	"reflect"
	"testing"
)

func TestLoadLightCurveMockConfigDefaults(
	t *testing.T,
) {
	t.Parallel()

	values := map[string]string{
		envLightCurveMockDataDir: " ./testdata ",
		envKafkaBrokers:          " kafka-1:9092, kafka-2:9092 ",
		envCandidateTopic:        " variable-star-candidate ",
	}

	config, err :=
		loadLightCurveMockConfig(
			mapEnvironmentLookup(values),
		)
	if err != nil {
		t.Fatalf(
			"loadLightCurveMockConfig() error = %v",
			err,
		)
	}

	if config.dataDir != "./testdata" {
		t.Fatalf(
			"dataDir = %q, want %q",
			config.dataDir,
			"./testdata",
		)
	}

	if config.listenAddr !=
		defaultLightCurveMockListenAddr {
		t.Fatalf(
			"listenAddr = %q, want %q",
			config.listenAddr,
			defaultLightCurveMockListenAddr,
		)
	}

	wantBrokers :=
		[]string{
			"kafka-1:9092",
			"kafka-2:9092",
		}

	if !reflect.DeepEqual(
		config.kafkaBrokers,
		wantBrokers,
	) {
		t.Fatalf(
			"kafkaBrokers = %#v, want %#v",
			config.kafkaBrokers,
			wantBrokers,
		)
	}

	if config.kafkaClientID !=
		defaultKafkaClientID {
		t.Fatalf(
			"kafkaClientID = %q, want %q",
			config.kafkaClientID,
			defaultKafkaClientID,
		)
	}

	if config.candidateTopic !=
		"variable-star-candidate" {
		t.Fatalf(
			"candidateTopic = %q, want %q",
			config.candidateTopic,
			"variable-star-candidate",
		)
	}

	if config.candidateRatePerSecond != 1 {
		t.Fatalf(
			"candidateRatePerSecond = %v, want 1",
			config.candidateRatePerSecond,
		)
	}
}

func TestLoadLightCurveMockConfigOverrides(
	t *testing.T,
) {
	t.Parallel()

	values := map[string]string{
		envLightCurveMockDataDir: "/srv/lightcurves",

		envLightCurveMockListenAddr: "0.0.0.0:18081",

		envKafkaBrokers: "kafka:9092",

		envKafkaClientID: "mock-client",

		envCandidateTopic: "candidate-test",

		envCandidateRatePerSecond: "12.5",
	}

	config, err :=
		loadLightCurveMockConfig(
			mapEnvironmentLookup(values),
		)
	if err != nil {
		t.Fatalf(
			"loadLightCurveMockConfig() error = %v",
			err,
		)
	}

	if config.listenAddr !=
		"0.0.0.0:18081" {
		t.Fatalf(
			"listenAddr = %q",
			config.listenAddr,
		)
	}

	if config.kafkaClientID !=
		"mock-client" {
		t.Fatalf(
			"kafkaClientID = %q",
			config.kafkaClientID,
		)
	}

	if config.candidateRatePerSecond !=
		12.5 {
		t.Fatalf(
			"candidateRatePerSecond = %v, want 12.5",
			config.candidateRatePerSecond,
		)
	}
}

func TestLoadLightCurveMockConfigRejectsMissingRequiredValues(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		values map[string]string
	}{
		{
			name: "missing data dir",
			values: map[string]string{
				envKafkaBrokers: "kafka:9092",

				envCandidateTopic: "candidate-test",
			},
		},
		{
			name: "missing brokers",
			values: map[string]string{
				envLightCurveMockDataDir: "./data",

				envCandidateTopic: "candidate-test",
			},
		},
		{
			name: "missing candidate topic",
			values: map[string]string{
				envLightCurveMockDataDir: "./data",

				envKafkaBrokers: "kafka:9092",
			},
		},
	}

	for _, test := range tests {
		test := test

		t.Run(
			test.name,
			func(t *testing.T) {
				t.Parallel()

				if _, err :=
					loadLightCurveMockConfig(
						mapEnvironmentLookup(
							test.values,
						),
					); err == nil {
					t.Fatal(
						"loadLightCurveMockConfig() error = nil",
					)
				}
			},
		)
	}
}

func TestLoadLightCurveMockConfigRejectsInvalidRate(
	t *testing.T,
) {
	t.Parallel()

	for _, rate := range []string{
		"0",
		"-1",
		"NaN",
		"+Inf",
		"not-a-number",
	} {
		rate := rate

		t.Run(
			rate,
			func(t *testing.T) {
				t.Parallel()

				values := map[string]string{
					envLightCurveMockDataDir: "./data",

					envKafkaBrokers: "kafka:9092",

					envCandidateTopic: "candidate-test",

					envCandidateRatePerSecond: rate,
				}

				if _, err :=
					loadLightCurveMockConfig(
						mapEnvironmentLookup(
							values,
						),
					); err == nil {
					t.Fatalf(
						"loadLightCurveMockConfig(rate=%q) error = nil",
						rate,
					)
				}
			},
		)
	}
}

func TestLoadLightCurveMockConfigRejectsEmptyBroker(
	t *testing.T,
) {
	t.Parallel()

	values := map[string]string{
		envLightCurveMockDataDir: "./data",

		envKafkaBrokers: "kafka-1:9092,,kafka-2:9092",

		envCandidateTopic: "candidate-test",
	}

	if _, err :=
		loadLightCurveMockConfig(
			mapEnvironmentLookup(values),
		); err == nil {
		t.Fatal(
			"loadLightCurveMockConfig() error = nil",
		)
	}
}

func mapEnvironmentLookup(
	values map[string]string,
) func(string) (string, bool) {
	return func(
		name string,
	) (string, bool) {
		value, ok := values[name]

		return value, ok
	}
}
