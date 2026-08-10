package main

import "testing"

func TestLightCurveMockConfigAllowsKafkaSASLDisabled(t *testing.T) {
	t.Parallel()

	config, err := loadLightCurveMockConfig(mapEnvironmentLookup(map[string]string{
		envLightCurveMockDataDir: "./data",
		envKafkaBrokers:          "kafka-1:19092,kafka-2:19092,kafka-3:19092",
		envCandidateTopic:        "variable-star-candidate",
	}))
	if err != nil {
		t.Fatalf("loadLightCurveMockConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "" || config.kafkaSASLPassword != "" {
		t.Fatalf(
			"Kafka SASL credentials = (%q, %q), want disabled",
			config.kafkaSASLUsername,
			config.kafkaSASLPassword,
		)
	}
}

func TestLightCurveMockConfigLoadsKafkaSASLCredentials(t *testing.T) {
	t.Parallel()

	config, err := loadLightCurveMockConfig(mapEnvironmentLookup(map[string]string{
		envLightCurveMockDataDir: "./data",
		envKafkaBrokers:          "kafka-1:19092,kafka-2:19092,kafka-3:19092",
		envCandidateTopic:        "variable-star-candidate",
		envKafkaSASLUsername:     " kafka-user ",
		envKafkaSASLPassword:     " secret-with-spaces ",
	}))
	if err != nil {
		t.Fatalf("loadLightCurveMockConfig() error = %v", err)
	}

	if config.kafkaSASLUsername != "kafka-user" {
		t.Fatalf("kafkaSASLUsername = %q, want %q", config.kafkaSASLUsername, "kafka-user")
	}
	if config.kafkaSASLPassword != " secret-with-spaces " {
		t.Fatal("kafkaSASLPassword was modified")
	}
}

func TestLightCurveMockConfigRejectsPartialKafkaSASL(t *testing.T) {
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

			values := map[string]string{
				envLightCurveMockDataDir: "./data",
				envKafkaBrokers:          "kafka-1:19092,kafka-2:19092,kafka-3:19092",
				envCandidateTopic:        "variable-star-candidate",
			}
			for key, value := range test.overrides {
				values[key] = value
			}

			if _, err := loadLightCurveMockConfig(mapEnvironmentLookup(values)); err == nil {
				t.Fatal("loadLightCurveMockConfig() error = nil")
			}
		})
	}
}
