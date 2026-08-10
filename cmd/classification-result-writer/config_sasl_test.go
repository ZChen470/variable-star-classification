package main

import "testing"

func TestClassificationResultWriterConfigAllowsKafkaSASLDisabled(t *testing.T) {
	t.Parallel()

	config, err := loadClassificationResultWriterConfig(resultWriterSASLTestLookup(nil))
	if err != nil {
		t.Fatalf("loadClassificationResultWriterConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "" || config.kafkaSASLPassword != "" {
		t.Fatalf(
			"Kafka SASL credentials = (%q, %q), want disabled",
			config.kafkaSASLUsername,
			config.kafkaSASLPassword,
		)
	}
}

func TestClassificationResultWriterConfigLoadsKafkaSASLCredentials(t *testing.T) {
	t.Parallel()

	config, err := loadClassificationResultWriterConfig(resultWriterSASLTestLookup(map[string]string{
		envKafkaSASLUsername: " kafka-user ",
		envKafkaSASLPassword: " secret-with-spaces ",
	}))
	if err != nil {
		t.Fatalf("loadClassificationResultWriterConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "kafka-user" {
		t.Fatalf("kafkaSASLUsername = %q, want %q", config.kafkaSASLUsername, "kafka-user")
	}
	if config.kafkaSASLPassword != " secret-with-spaces " {
		t.Fatal("kafkaSASLPassword was modified")
	}
}

func TestClassificationResultWriterConfigRejectsPartialKafkaSASL(t *testing.T) {
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

			if _, err := loadClassificationResultWriterConfig(
				resultWriterSASLTestLookup(test.overrides),
			); err == nil {
				t.Fatal("loadClassificationResultWriterConfig() error = nil")
			}
		})
	}
}

func resultWriterSASLTestLookup(overrides map[string]string) func(string) (string, bool) {
	values := map[string]string{
		envKafkaBrokers:                 "kafka-1:19092,kafka-2:19092,kafka-3:19092",
		envKafkaConsumerGroup:           "light-curve-classification-result-group",
		envClassificationResultTopic:    "light-curve-classification-result",
		envClassificationResultDLQTopic: "light-curve-classification-result-dlq",
		envPostgresDSN:                  "postgres://test:test@astro-postgres:5432/test",
	}

	for key, value := range overrides {
		values[key] = value
	}

	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
