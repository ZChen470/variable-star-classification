package main

import "testing"

func TestCandidateOrchestratorManagementListenAddrDefault(
	t *testing.T,
) {
	t.Parallel()

	config, err := loadCandidateOrchestratorConfig(
		environmentLookup(
			validCandidateOrchestratorEnvironment(),
		),
	)
	if err != nil {
		t.Fatalf(
			"loadCandidateOrchestratorConfig() error = %v",
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

func TestCandidateOrchestratorManagementListenAddrOverride(
	t *testing.T,
) {
	t.Parallel()

	values := validCandidateOrchestratorEnvironment()
	values[envManagementListenAddr] =
		"  127.0.0.1:19091  "

	config, err := loadCandidateOrchestratorConfig(
		environmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadCandidateOrchestratorConfig() error = %v",
			err,
		)
	}

	if config.managementListenAddr !=
		"127.0.0.1:19091" {
		t.Fatalf(
			"management listen addr = %q, want %q",
			config.managementListenAddr,
			"127.0.0.1:19091",
		)
	}
}

func TestCandidateOrchestratorBlankManagementListenAddrUsesDefault(
	t *testing.T,
) {
	t.Parallel()

	values := validCandidateOrchestratorEnvironment()
	values[envManagementListenAddr] = "   "

	config, err := loadCandidateOrchestratorConfig(
		environmentLookup(values),
	)
	if err != nil {
		t.Fatalf(
			"loadCandidateOrchestratorConfig() error = %v",
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
