package application

import (
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestNewClassificationPolicyV1RejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		modelBundleVersion      string
		classificationPolicyVer string
		wantError               string
	}{
		{
			name:                    "empty model bundle version",
			modelBundleVersion:      "",
			classificationPolicyVer: "classification-policy-v1",
			wantError:               "model bundle version must not be empty",
		},
		{
			name:                    "model bundle version with whitespace",
			modelBundleVersion:      " bundle-v1",
			classificationPolicyVer: "classification-policy-v1",
			wantError:               "model bundle version must not contain leading or trailing whitespace",
		},
		{
			name:                    "model bundle version with NUL",
			modelBundleVersion:      "bundle\x00-v1",
			classificationPolicyVer: "classification-policy-v1",
			wantError:               "model bundle version must not contain NUL",
		},
		{
			name:                    "empty classification policy version",
			modelBundleVersion:      "bundle-v1",
			classificationPolicyVer: "",
			wantError:               "classification policy version must not be empty",
		},
		{
			name:                    "classification policy version with whitespace",
			modelBundleVersion:      "bundle-v1",
			classificationPolicyVer: "classification-policy-v1 ",
			wantError:               "classification policy version must not contain leading or trailing whitespace",
		},
		{
			name:                    "classification policy version with NUL",
			modelBundleVersion:      "bundle-v1",
			classificationPolicyVer: "classification-policy\x00-v1",
			wantError:               "classification policy version must not contain NUL",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewClassificationPolicyV1(
				test.modelBundleVersion,
				test.classificationPolicyVer,
			)
			if err.Error() != test.wantError {
				t.Fatalf(
					"unexpected error: got %q, want %q",
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
		"classification-policy-v1",
	)
	if err != nil {
		t.Fatalf("NewClassificationPolicyV1 returned error: %v", err)
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

			decision, err := policy.Evaluate(CandidateEventInput{
				EventType:          test.eventType,
				EligibleEpochCount: test.eligibleEpochs,
			})

			if test.wantError {
				if err == nil {
					t.Fatal("expected policy evaluation error")
				}
				return
			}

			if err != nil {
				t.Fatalf("Evaluate returned error: %v", err)
			}

			if decision.ShouldClassify != test.wantClassify {
				t.Fatalf(
					"unexpected ShouldClassify: got %t, want %t",
					decision.ShouldClassify,
					test.wantClassify,
				)
			}

			if !test.wantClassify {
				if decision != (ClassificationPolicyDecision{}) {
					t.Fatalf(
						"unexpected non-classification decision: got %+v",
						decision,
					)
				}
				return
			}

			if decision.ModelBundleVersion != "variable-classifier-2026-07-001" {
				t.Fatalf(
					"unexpected model bundle version: %q",
					decision.ModelBundleVersion,
				)
			}
			if decision.ClassificationPolicyVersion != "classification-policy-v1" {
				t.Fatalf(
					"unexpected classification policy version: %q",
					decision.ClassificationPolicyVersion,
				)
			}
			if decision.ExecutionMode != domain.ExecutionModeProduction {
				t.Fatalf(
					"unexpected execution mode: got %d, want %d",
					decision.ExecutionMode,
					domain.ExecutionModeProduction,
				)
			}
			if decision.Priority != ClassificationPriorityRealtime {
				t.Fatalf(
					"unexpected priority: got %d, want %d",
					decision.Priority,
					ClassificationPriorityRealtime,
				)
			}
			if decision.DeadlineAt != nil {
				t.Fatalf(
					"expected nil deadline, got %v",
					decision.DeadlineAt,
				)
			}
		})
	}
}

func TestClassificationPolicyV1ZeroValueIsNotUsable(t *testing.T) {
	t.Parallel()

	var policy ClassificationPolicyV1

	_, err := policy.Evaluate(CandidateEventInput{
		EventType:          CandidateEventTypeCreated,
		EligibleEpochCount: MinimumEligibleEpochCount,
	})
	if err == nil {
		t.Fatal("expected zero-value policy configuration error")
	}
}
