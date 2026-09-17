package main

import (
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv(
		envKafkaBrokers,
		" kafka-1:9092, kafka-2:9092 ",
	)
	t.Setenv(
		envKafkaConsumerGroup,
		" command-group ",
	)
	t.Setenv(
		envKafkaTopic,
		" command-topic ",
	)
	t.Setenv(
		envKafkaSASLUsername,
		"astro",
	)
	t.Setenv(
		envKafkaSASLPassword,
		"secret",
	)
	t.Setenv(
		envKafkaClientID,
		" backlog-observer ",
	)
	t.Setenv(
		envRefreshInterval,
		"20s",
	)
	t.Setenv(
		envRefreshTimeout,
		"7s",
	)
	t.Setenv(
		envManagementListenAddr,
		"127.0.0.1:19091",
	)

	got, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if len(got.kafkaBrokers) != 2 ||
		got.kafkaBrokers[0] != "kafka-1:9092" ||
		got.kafkaBrokers[1] != "kafka-2:9092" {
		t.Fatalf(
			"kafka brokers = %#v",
			got.kafkaBrokers,
		)
	}
	if got.kafkaConsumerGroup != "command-group" {
		t.Fatalf(
			"consumer group = %q",
			got.kafkaConsumerGroup,
		)
	}
	if got.kafkaTopic != "command-topic" {
		t.Fatalf(
			"topic = %q",
			got.kafkaTopic,
		)
	}
	if got.kafkaSASLUsername != "astro" {
		t.Fatalf(
			"SASL username = %q",
			got.kafkaSASLUsername,
		)
	}
	if got.kafkaSASLPassword != "secret" {
		t.Fatal("SASL password mismatch")
	}
	if got.kafkaClientID != "backlog-observer" {
		t.Fatalf(
			"client ID = %q",
			got.kafkaClientID,
		)
	}
	if got.refreshInterval != 20*time.Second {
		t.Fatalf(
			"refresh interval = %v",
			got.refreshInterval,
		)
	}
	if got.refreshTimeout != 7*time.Second {
		t.Fatalf(
			"refresh timeout = %v",
			got.refreshTimeout,
		)
	}
	if got.managementListenAddr != "127.0.0.1:19091" {
		t.Fatalf(
			"management listen addr = %q",
			got.managementListenAddr,
		)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearConfigEnv(t)

	t.Setenv(
		envKafkaBrokers,
		"kafka-1:9092",
	)
	t.Setenv(
		envKafkaConsumerGroup,
		"command-group",
	)
	t.Setenv(
		envKafkaTopic,
		"command-topic",
	)

	got, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	if got.kafkaClientID != defaultKafkaClientID {
		t.Fatalf(
			"client ID = %q, want %q",
			got.kafkaClientID,
			defaultKafkaClientID,
		)
	}
	if got.refreshInterval != defaultRefreshInterval {
		t.Fatalf(
			"refresh interval = %v, want %v",
			got.refreshInterval,
			defaultRefreshInterval,
		)
	}
	if got.refreshTimeout != defaultRefreshTimeout {
		t.Fatalf(
			"refresh timeout = %v, want %v",
			got.refreshTimeout,
			defaultRefreshTimeout,
		)
	}
	if got.managementListenAddr !=
		defaultManagementListenAddr {
		t.Fatalf(
			"management listen addr = %q, want %q",
			got.managementListenAddr,
			defaultManagementListenAddr,
		)
	}
}

func TestLoadConfigRejectsInvalidConfiguration(
	t *testing.T,
) {
	tests := []struct {
		name string
		set  func(*testing.T)
	}{
		{
			name: "missing brokers",
			set: func(t *testing.T) {
				t.Setenv(
					envKafkaConsumerGroup,
					"group",
				)
				t.Setenv(
					envKafkaTopic,
					"topic",
				)
			},
		},
		{
			name: "missing group",
			set: func(t *testing.T) {
				t.Setenv(
					envKafkaBrokers,
					"kafka:9092",
				)
				t.Setenv(
					envKafkaTopic,
					"topic",
				)
			},
		},
		{
			name: "missing topic",
			set: func(t *testing.T) {
				t.Setenv(
					envKafkaBrokers,
					"kafka:9092",
				)
				t.Setenv(
					envKafkaConsumerGroup,
					"group",
				)
			},
		},
		{
			name: "SASL username without password",
			set: func(t *testing.T) {
				setRequiredConfigEnv(t)
				t.Setenv(
					envKafkaSASLUsername,
					"astro",
				)
			},
		},
		{
			name: "SASL password without username",
			set: func(t *testing.T) {
				setRequiredConfigEnv(t)
				t.Setenv(
					envKafkaSASLPassword,
					"secret",
				)
			},
		},
		{
			name: "invalid refresh interval",
			set: func(t *testing.T) {
				setRequiredConfigEnv(t)
				t.Setenv(
					envRefreshInterval,
					"invalid",
				)
			},
		},
		{
			name: "zero refresh interval",
			set: func(t *testing.T) {
				setRequiredConfigEnv(t)
				t.Setenv(
					envRefreshInterval,
					"0s",
				)
			},
		},
		{
			name: "negative refresh timeout",
			set: func(t *testing.T) {
				setRequiredConfigEnv(t)
				t.Setenv(
					envRefreshTimeout,
					"-1s",
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnv(t)
			test.set(t)

			if _, err := loadConfig(); err == nil {
				t.Fatal("loadConfig() error = nil")
			}
		})
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()

	for _, name := range []string{
		envKafkaBrokers,
		envKafkaConsumerGroup,
		envKafkaTopic,
		envKafkaSASLUsername,
		envKafkaSASLPassword,
		envKafkaClientID,
		envRefreshInterval,
		envRefreshTimeout,
		envManagementListenAddr,
	} {
		t.Setenv(name, "")
	}
}

func setRequiredConfigEnv(t *testing.T) {
	t.Helper()

	t.Setenv(
		envKafkaBrokers,
		"kafka:9092",
	)
	t.Setenv(
		envKafkaConsumerGroup,
		"group",
	)
	t.Setenv(
		envKafkaTopic,
		"topic",
	)
}
