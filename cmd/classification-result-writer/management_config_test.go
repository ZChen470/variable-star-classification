package main

import "testing"

func TestClassificationResultWriterManagementListenAddrDefault(
	t *testing.T,
) {
	t.Parallel()

	config, err := loadClassificationResultWriterConfig(
		classificationResultWriterEnvironmentLookup(
			validClassificationResultWriterEnvironment(),
		),
	)
	if err != nil {
		t.Fatalf(
			"loadClassificationResultWriterConfig() error = %v",
			err,
		)
	}

	if config.managementListenAddr !=
		defaultManagementListenAddr {
		t.Fatalf(
			"management listen addr = %q, want %q",
			config.managementListenAddr,
			defaultManagementListenAddr,
		)
	}
}

func TestClassificationResultWriterManagementListenAddrOverride(
	t *testing.T,
) {
	t.Parallel()

	values := validClassificationResultWriterEnvironment()

	values[envManagementListenAddr] =
		"  127.0.0.1:19093  "

	config, err := loadClassificationResultWriterConfig(
		classificationResultWriterEnvironmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadClassificationResultWriterConfig() error = %v",
			err,
		)
	}

	if config.managementListenAddr !=
		"127.0.0.1:19093" {
		t.Fatalf(
			"management listen addr = %q, want %q",
			config.managementListenAddr,
			"127.0.0.1:19093",
		)
	}
}

func TestClassificationResultWriterBlankManagementListenAddrUsesDefault(
	t *testing.T,
) {
	t.Parallel()

	values := validClassificationResultWriterEnvironment()
	values[envManagementListenAddr] = "   "

	config, err := loadClassificationResultWriterConfig(
		classificationResultWriterEnvironmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadClassificationResultWriterConfig() error = %v",
			err,
		)
	}

	if config.managementListenAddr !=
		defaultManagementListenAddr {
		t.Fatalf(
			"management listen addr = %q, want %q",
			config.managementListenAddr,
			defaultManagementListenAddr,
		)
	}
}

func validClassificationResultWriterEnvironment() map[string]string {
	return map[string]string{
		envKafkaBrokers:                 "127.0.0.1:9092",
		envKafkaConsumerGroup:           "classification-result-writer-test-group",
		envClassificationResultTopic:    "classification-result-test",
		envClassificationResultDLQTopic: "classification-result-dlq-test",
		envPostgresDSN:                  "postgres://user:password@127.0.0.1/testdb",
	}
}

func classificationResultWriterEnvironmentLookup(
	values map[string]string,
) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
