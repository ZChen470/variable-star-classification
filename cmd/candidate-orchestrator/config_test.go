package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadCandidateOrchestratorConfig(t *testing.T) {
	t.Parallel()

	values := validCandidateOrchestratorEnvironment()
	values[envKafkaBrokers] = "broker-a:9092, broker-b:9092"
	values[envModelBundleVersion] = " bundle-v1 "
	values[envKafkaConsumerGroup] =
		" candidate-orchestrator-v1 "
	values[envCandidateTopic] =
		" astro.candidate.events.v1 "
	values[envClassificationCommandTopic] =
		" astro.classification.commands.v1 "
	values[envCandidateDLQTopic] =
		" astro.candidate.events.dlq.v1 "

	config, err := loadCandidateOrchestratorConfig(
		environmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadCandidateOrchestratorConfig returned error: %v",
			err,
		)
	}

	wantBrokers := []string{
		"broker-a:9092",
		"broker-b:9092",
	}
	if !reflect.DeepEqual(config.kafkaBrokers, wantBrokers) {
		t.Fatalf(
			"kafka brokers = %#v, want %#v",
			config.kafkaBrokers,
			wantBrokers,
		)
	}

	if config.kafkaConsumerGroup != "candidate-orchestrator-v1" {
		t.Fatalf(
			"consumer group = %q, want %q",
			config.kafkaConsumerGroup,
			"candidate-orchestrator-v1",
		)
	}

	if config.kafkaClientID != defaultKafkaClientID {
		t.Fatalf(
			"client ID = %q, want %q",
			config.kafkaClientID,
			defaultKafkaClientID,
		)
	}

	if config.candidateTopic != "astro.candidate.events.v1" {
		t.Fatalf(
			"candidate topic = %q, want trimmed value",
			config.candidateTopic,
		)
	}

	if config.classificationCommandTopic !=
		"astro.classification.commands.v1" {
		t.Fatalf(
			"command topic = %q, want trimmed value",
			config.classificationCommandTopic,
		)
	}

	if config.candidateDLQTopic !=
		"astro.candidate.events.dlq.v1" {
		t.Fatalf(
			"DLQ topic = %q, want trimmed value",
			config.candidateDLQTopic,
		)
	}

	// 版本字段参与确定性身份计算，不允许加载器自动 TrimSpace。
	if config.modelBundleVersion != " bundle-v1 " {
		t.Fatalf(
			"model bundle version = %q, want preserved value",
			config.modelBundleVersion,
		)
	}
}

func TestLoadCandidateOrchestratorConfigUsesClientIDOverride(
	t *testing.T,
) {
	t.Parallel()

	values := validCandidateOrchestratorEnvironment()
	values[envKafkaClientID] = "  candidate-runtime  "

	config, err := loadCandidateOrchestratorConfig(
		environmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadCandidateOrchestratorConfig returned error: %v",
			err,
		)
	}

	if config.kafkaClientID != "candidate-runtime" {
		t.Fatalf(
			"client ID = %q, want %q",
			config.kafkaClientID,
			"candidate-runtime",
		)
	}
}

func TestLoadCandidateOrchestratorConfigRequiresEnvironment(
	t *testing.T,
) {
	t.Parallel()

	requiredNames := []string{
		envKafkaBrokers,
		envKafkaConsumerGroup,
		envCandidateTopic,
		envClassificationCommandTopic,
		envCandidateDLQTopic,
		envModelBundleVersion,
	}

	for _, name := range requiredNames {
		name := name

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			values := validCandidateOrchestratorEnvironment()
			delete(values, name)

			_, err := loadCandidateOrchestratorConfig(
				environmentLookup(values),
			)
			if err == nil {
				t.Fatalf(
					"expected missing %s error",
					name,
				)
			}

			if !strings.Contains(err.Error(), name) {
				t.Fatalf(
					"error = %q, want environment name %q",
					err,
					name,
				)
			}
		})
	}
}

func TestLoadCandidateOrchestratorConfigRejectsEmptyBroker(
	t *testing.T,
) {
	t.Parallel()

	values := validCandidateOrchestratorEnvironment()
	values[envKafkaBrokers] =
		"broker-a:9092,,broker-b:9092"

	_, err := loadCandidateOrchestratorConfig(
		environmentLookup(values),
	)
	if err == nil {
		t.Fatal("expected empty broker error")
	}

	if !strings.Contains(err.Error(), "empty broker") {
		t.Fatalf(
			"error = %q, want empty broker error",
			err,
		)
	}
}

func TestLoadCandidateOrchestratorConfigRejectsNilLookup(
	t *testing.T,
) {
	t.Parallel()

	_, err := loadCandidateOrchestratorConfig(nil)
	if err == nil {
		t.Fatal("expected nil environment lookup error")
	}
}

func validCandidateOrchestratorEnvironment() map[string]string {
	return map[string]string{
		envKafkaBrokers:               "broker:9092",
		envKafkaConsumerGroup:         "candidate-orchestrator-v1",
		envCandidateTopic:             "astro.candidate.events.v1",
		envClassificationCommandTopic: "astro.classification.commands.v1",
		envCandidateDLQTopic:          "astro.candidate.events.dlq.v1",
		envModelBundleVersion:         "bundle-v1",
	}
}

func environmentLookup(
	values map[string]string,
) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
