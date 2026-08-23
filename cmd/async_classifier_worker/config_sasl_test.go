package main

import "testing"

func TestClassifierWorkerConfigAllowsKafkaSASLDisabled(t *testing.T) {
	t.Parallel()

	config, err := loadClassifierWorkerConfig(workerSASLTestLookup(nil))
	if err != nil {
		t.Fatalf("loadClassifierWorkerConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "" || config.kafkaSASLPassword != "" {
		t.Fatalf(
			"Kafka SASL credentials = (%q, %q), want disabled",
			config.kafkaSASLUsername,
			config.kafkaSASLPassword,
		)
	}
}

func TestClassifierWorkerConfigLoadsKafkaSASLCredentials(t *testing.T) {
	t.Parallel()

	config, err := loadClassifierWorkerConfig(workerSASLTestLookup(map[string]string{
		envKafkaSASLUsername: " kafka-user ",
		envKafkaSASLPassword: " secret-with-spaces ",
	}))
	if err != nil {
		t.Fatalf("loadClassifierWorkerConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "kafka-user" {
		t.Fatalf("kafkaSASLUsername = %q, want %q", config.kafkaSASLUsername, "kafka-user")
	}
	if config.kafkaSASLPassword != " secret-with-spaces " {
		t.Fatal("kafkaSASLPassword was modified")
	}
}

func TestClassifierWorkerConfigRejectsPartialKafkaSASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]string
	}{
		{
			name: "username only",
			overrides: map[string]string{
				envKafkaSASLUsername: "kafka-user",
			},
		},
		{
			name: "password only",
			overrides: map[string]string{
				envKafkaSASLPassword: "secret",
			},
		},
		{
			name: "blank username",
			overrides: map[string]string{
				envKafkaSASLUsername: " ",
				envKafkaSASLPassword: "secret",
			},
		},
		{
			name: "blank password",
			overrides: map[string]string{
				envKafkaSASLUsername: "kafka-user",
				envKafkaSASLPassword: " ",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := loadClassifierWorkerConfig(workerSASLTestLookup(test.overrides)); err == nil {
				t.Fatal("loadClassifierWorkerConfig() error = nil")
			}
		})
	}
}

func workerSASLTestLookup(overrides map[string]string) func(string) (string, bool) {
	values := map[string]string{
		envKafkaBrokers:                  "kafka-1:19092,kafka-2:19092,kafka-3:19092",
		envKafkaConsumerGroup:            "light-curve-classification-command-group",
		envClassificationCommandTopic:    "light-curve-classification-command",
		envClassificationResultTopic:     "light-curve-classification-result",
		envClassificationCommandDLQTopic: "light-curve-classification-command-dlq",
		envModelBundleVersion:            "test-bundle",
		envModelBundleManifestPath:       "/config/model-bundle-manifest-v2.yaml",
		envTritonBaseURL:                 "http://triton:8000",
		envPostgresDSN:                   "postgres://test:test@astro-postgres:5432/test",
		envLightCurveBaseURL:             "http://lightcurve-mock-server:18081",
	}

	for key, value := range overrides {
		values[key] = value
	}

	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
