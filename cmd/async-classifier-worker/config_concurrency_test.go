package main

import "testing"

func TestClassifierWorkerConfigDefaultsConcurrency(t *testing.T) {
	t.Parallel()

	config, err := loadClassifierWorkerConfig(workerSASLTestLookup(nil))
	if err != nil {
		t.Fatalf("loadClassifierWorkerConfig() error = %v", err)
	}

	if config.classifierWorkerConcurrency != 1 {
		t.Fatalf(
			"concurrency=%d want=1",
			config.classifierWorkerConcurrency,
		)
	}
}

func TestClassifierWorkerConfigLoadsConcurrency(t *testing.T) {
	t.Parallel()

	config, err := loadClassifierWorkerConfig(workerSASLTestLookup(map[string]string{
		envClassifierWorkerConcurrency: "8",
	}))
	if err != nil {
		t.Fatalf("loadClassifierWorkerConfig() error = %v", err)
	}

	if config.classifierWorkerConcurrency != 8 {
		t.Fatalf(
			"concurrency=%d want=8",
			config.classifierWorkerConcurrency,
		)
	}
}

func TestClassifierWorkerConfigRejectsInvalidConcurrency(t *testing.T) {
	t.Parallel()

	tests := []string{
		"0",
		"-1",
		"65",
		"abc",
	}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			if _, err := loadClassifierWorkerConfig(workerSASLTestLookup(map[string]string{
				envClassifierWorkerConcurrency: value,
			})); err == nil {
				t.Fatalf(
					"expected error for concurrency=%q",
					value,
				)
			}
		})
	}
}
