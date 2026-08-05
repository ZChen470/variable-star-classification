package application

import (
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestNewClassificationPolicyV1RejectsInvalidConfiguration(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name               string
		modelBundleVersion string
		wantError          string
	}{
		{
			name:               "empty model bundle version",
			modelBundleVersion: "",
			wantError:          "model bundle version must not be empty",
		},
		{
			name:               "model bundle version with whitespace",
			modelBundleVersion: " bundle-v1",
			wantError:          "model bundle version must not contain leading or trailing whitespace",
		},
		{
			name:               "model bundle version with NUL",
			modelBundleVersion: "bundle\x00-v1",
			wantError:          "model bundle version must not contain NUL",
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewClassificationPolicyV1(
				test.modelBundleVersion,
			)
			if err == nil {
				t.Fatalf(
					"NewClassificationPolicyV1() error = nil, want %q",
					test.wantError,
				)
			}

			if err.Error() != test.wantError {
				t.Fatalf(
					"NewClassificationPolicyV1() error = %q, want %q",
					err.Error(),
					test.wantError,
				)
			}
		})
	}
}

func TestClassificationPolicyV1Evaluate(t *testing.T) {
	t.Parallel()

	policy, err := NewClassificationPolicyV1(
		"variable-classifier-2026-07-001",
	)
	if err != nil {
		t.Fatalf(
			"NewClassificationPolicyV1() error = %v",
			err,
		)
	}

	tests := []struct {
		name           string
		eventType      CandidateEventType
		eligibleEpochs uint32
		wantClassify   bool
		wantError      bool
	}{
		{
			name:           "created at minimum epoch boundary",
			eventType:      CandidateEventTypeCreated,
			eligibleEpochs: MinimumEligibleEpochCount,
			wantClassify:   true,
		},
		{
			name:           "updated at minimum epoch boundary",
			eventType:      CandidateEventTypeUpdated,
			eligibleEpochs: MinimumEligibleEpochCount,
			wantClassify:   true,
		},
		{
			name:           "created below minimum epochs",
			eventType:      CandidateEventTypeCreated,
			eligibleEpochs: MinimumEligibleEpochCount - 1,
			wantClassify:   false,
		},
		{
			name:           "unsupported internal event type",
			eventType:      CandidateEventType(99),
			eligibleEpochs: MinimumEligibleEpochCount,
			wantError:      true,
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decision, err := policy.Evaluate(
				CandidateEventInput{
					EventType:          test.eventType,
					EligibleEpochCount: test.eligibleEpochs,
				},
			)

			if test.wantError {
				if err == nil {
					t.Fatal(
						"Evaluate() error = nil, want non-nil",
					)
				}
				return
			}

			if err != nil {
				t.Fatalf(
					"Evaluate() error = %v",
					err,
				)
			}

			if decision.ShouldClassify != test.wantClassify {
				t.Fatalf(
					"ShouldClassify = %t, want %t",
					decision.ShouldClassify,
					test.wantClassify,
				)
			}

			if !test.wantClassify {
				if decision !=
					(ClassificationPolicyDecision{}) {
					t.Fatalf(
						"decision = %+v, want zero",
						decision,
					)
				}
				return
			}

			if decision.ModelBundleVersion !=
				"variable-classifier-2026-07-001" {
				t.Fatalf(
					"ModelBundleVersion = %q",
					decision.ModelBundleVersion,
				)
			}

			if decision.ExecutionMode !=
				domain.ExecutionModeProduction {
				t.Fatalf(
					"ExecutionMode = %d, want %d",
					decision.ExecutionMode,
					domain.ExecutionModeProduction,
				)
			}

			if decision.Priority !=
				ClassificationPriorityRealtime {
				t.Fatalf(
					"Priority = %d, want %d",
					decision.Priority,
					ClassificationPriorityRealtime,
				)
			}

			if decision.DeadlineAt != nil {
				t.Fatalf(
					"DeadlineAt = %v, want nil",
					decision.DeadlineAt,
				)
			}
		})
	}
}

func TestClassificationPolicyV1ZeroValueIsNotUsable(
	t *testing.T,
) {
	t.Parallel()

	var policy ClassificationPolicyV1

	_, err := policy.Evaluate(CandidateEventInput{
		EventType:          CandidateEventTypeCreated,
		EligibleEpochCount: MinimumEligibleEpochCount,
	})
	if err == nil {
		t.Fatal(
			"Evaluate() error = nil, want configuration error",
		)
	}
}
