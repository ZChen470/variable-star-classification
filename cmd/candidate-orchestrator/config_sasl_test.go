package main

import "testing"

func TestCandidateOrchestratorConfigAllowsKafkaSASLDisabled(t *testing.T) {
	t.Parallel()

	config, err := loadCandidateOrchestratorConfig(candidateSASLTestLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("loadCandidateOrchestratorConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "" || config.kafkaSASLPassword != "" {
		t.Fatalf(
			"Kafka SASL credentials = (%q, %q), want disabled",
			config.kafkaSASLUsername,
			config.kafkaSASLPassword,
		)
	}
}

func TestCandidateOrchestratorConfigLoadsKafkaSASLCredentials(t *testing.T) {
	t.Parallel()

	config, err := loadCandidateOrchestratorConfig(candidateSASLTestLookup(map[string]string{
		envKafkaSASLUsername: " kafka-user ",
		envKafkaSASLPassword: " secret-with-spaces ",
	}))
	if err != nil {
		t.Fatalf("loadCandidateOrchestratorConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "kafka-user" {
		t.Fatalf("kafkaSASLUsername = %q, want %q", config.kafkaSASLUsername, "kafka-user")
	}

	if config.kafkaSASLPassword != " secret-with-spaces " {
		t.Fatalf("kafkaSASLPassword was modified")
	}
}

func TestCandidateOrchestratorConfigRejectsPartialKafkaSASL(t *testing.T) {
	t.Parallel()

	tests := []map[string]string{
		{envKafkaSASLUsername: "kafka-user"},
		{envKafkaSASLPassword: "secret"},
		{
			envKafkaSASLUsername: " ",
			envKafkaSASLPassword: "secret",
		},
		{
			envKafkaSASLUsername: "kafka-user",
			envKafkaSASLPassword: " ",
		},
	}

	for _, overrides := range tests {
		overrides := overrides

		t.Run("invalid credentials", func(t *testing.T) {
			t.Parallel()

			if _, err := loadCandidateOrchestratorConfig(candidateSASLTestLookup(overrides)); err == nil {
				t.Fatal("loadCandidateOrchestratorConfig() error = nil")
			}
		})
	}
}

func candidateSASLTestLookup(overrides map[string]string) func(string) (string, bool) {
	values := map[string]string{
		envKafkaBrokers:               "kafka-1:19092,kafka-2:19092,kafka-3:19092",
		envKafkaConsumerGroup:         "variable-star-candidate-group",
		envCandidateTopic:             "variable-star-candidate",
		envClassificationCommandTopic: "light-curve-classification-command",
		envCandidateDLQTopic:          "variable-star-candidate-dlq",
		envModelBundleVersion:         "test-bundle",
	}

	for key, value := range overrides {
		values[key] = value
	}

	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
