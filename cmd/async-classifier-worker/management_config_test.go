package main

import "testing"

func TestClassifierWorkerManagementListenAddrDefault(
	t *testing.T,
) {
	t.Parallel()

	config, err := loadClassifierWorkerConfig(
		classifierWorkerEnvironmentLookup(
			validClassifierWorkerEnvironment(),
		),
	)
	if err != nil {
		t.Fatalf(
			"loadClassifierWorkerConfig() error = %v",
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

func TestClassifierWorkerManagementListenAddrOverride(
	t *testing.T,
) {
	t.Parallel()

	values := validClassifierWorkerEnvironment()
	values[envManagementListenAddr] =
		"  127.0.0.1:19092  "

	config, err := loadClassifierWorkerConfig(
		classifierWorkerEnvironmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadClassifierWorkerConfig() error = %v",
			err,
		)
	}

	if config.managementListenAddr !=
		"127.0.0.1:19092" {
		t.Fatalf(
			"management listen addr = %q, want %q",
			config.managementListenAddr,
			"127.0.0.1:19092",
		)
	}
}

func TestClassifierWorkerBlankManagementListenAddrUsesDefault(
	t *testing.T,
) {
	t.Parallel()

	values := validClassifierWorkerEnvironment()
	values[envManagementListenAddr] = "   "

	config, err := loadClassifierWorkerConfig(
		classifierWorkerEnvironmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadClassifierWorkerConfig() error = %v",
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

func validClassifierWorkerEnvironment() map[string]string {
	return map[string]string{
		envKafkaBrokers:                  "127.0.0.1:9092",
		envKafkaConsumerGroup:            "classifier-worker-test-group",
		envClassificationCommandTopic:    "classification-command-test",
		envClassificationResultTopic:     "classification-result-test",
		envClassificationCommandDLQTopic: "classification-command-dlq-test",
		envModelBundleVersion:            "bundle-v1",
		envModelBundleManifestPath:       "testdata/model-bundle-manifest.yaml",
		envTritonBaseURL:                 "http://127.0.0.1:8000",
		envPostgresDSN:                   "postgres://user:password@127.0.0.1/testdb",
		envLightCurveBaseURL:             "http://127.0.0.1:8080",
	}
}

func classifierWorkerEnvironmentLookup(
	values map[string]string,
) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
