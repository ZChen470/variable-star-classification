package domain

import "testing"

func TestDeterministicIDsGoldenVector(t *testing.T) {
	identity := JobIdentity{
		ObjectID:                    "OBJ-0001",
		LightCurveRevision:          21,
		ModelBundleVersion:          "bundle-2026-07-001",
		ClassificationPolicyVersion: "classification-policy-v1",
		ExecutionMode:               ExecutionModeProduction,
	}

	jobID, err := GenerateJobID(identity)
	if err != nil {
		t.Fatalf("GenerateJobID() error = %v", err)
	}

	const wantJobID = JobID("46709af9-e19b-5dfc-beb5-b9213127fd18")
	if jobID != wantJobID {
		t.Fatalf("GenerateJobID() = %q, want %q", jobID, wantJobID)
	}

	runID, err := GenerateRunID(jobID)
	if err != nil {
		t.Fatalf("GenerateRunID() error = %v", err)
	}

	const wantRunID = RunID("d42c8015-e1f6-59df-b3ad-3e0f3cff2702")
	if runID != wantRunID {
		t.Fatalf("GenerateRunID() = %q, want %q", runID, wantRunID)
	}
}

func TestGenerateJobIDIsDeterministic(t *testing.T) {
	identity := JobIdentity{
		ObjectID:                    "OBJ-0001",
		LightCurveRevision:          21,
		ModelBundleVersion:          "bundle-2026-07-001",
		ClassificationPolicyVersion: "classification-policy-v1",
		ExecutionMode:               ExecutionModeProduction,
	}

	first, err := GenerateJobID(identity)
	if err != nil {
		t.Fatalf("first GenerateJobID() error = %v", err)
	}

	second, err := GenerateJobID(identity)
	if err != nil {
		t.Fatalf("second GenerateJobID() error = %v", err)
	}

	if first != second {
		t.Fatalf("GenerateJobID() is not deterministic: %q != %q", first, second)
	}
}

func TestGenerateJobIDChangesWhenIdentityChanges(t *testing.T) {
	base := JobIdentity{
		ObjectID:                    "OBJ-0001",
		LightCurveRevision:          21,
		ModelBundleVersion:          "bundle-2026-07-001",
		ClassificationPolicyVersion: "classification-policy-v1",
		ExecutionMode:               ExecutionModeProduction,
	}

	baseID, err := GenerateJobID(base)
	if err != nil {
		t.Fatalf("GenerateJobID(base) error = %v", err)
	}

	tests := []struct {
		name     string
		identity JobIdentity
	}{
		{
			name: "object ID",
			identity: func() JobIdentity {
				changed := base
				changed.ObjectID = "OBJ-0002"
				return changed
			}(),
		},
		{
			name: "light curve revision",
			identity: func() JobIdentity {
				changed := base
				changed.LightCurveRevision = 22
				return changed
			}(),
		},
		{
			name: "model bundle version",
			identity: func() JobIdentity {
				changed := base
				changed.ModelBundleVersion = "bundle-2026-07-002"
				return changed
			}(),
		},
		{
			name: "classification policy version",
			identity: func() JobIdentity {
				changed := base
				changed.ClassificationPolicyVersion = "classification-policy-v2"
				return changed
			}(),
		},
		{
			name: "execution mode",
			identity: func() JobIdentity {
				changed := base
				changed.ExecutionMode = ExecutionModeShadow
				return changed
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changedID, err := GenerateJobID(tt.identity)
			if err != nil {
				t.Fatalf("GenerateJobID() error = %v", err)
			}
			if changedID == baseID {
				t.Fatalf(
					"GenerateJobID() did not change when %s changed: %q",
					tt.name,
					changedID,
				)
			}
		})
	}
}

func TestGenerateJobIDRejectsInvalidIdentity(t *testing.T) {
	valid := JobIdentity{
		ObjectID:                    "OBJ-0001",
		LightCurveRevision:          21,
		ModelBundleVersion:          "bundle-2026-07-001",
		ClassificationPolicyVersion: "classification-policy-v1",
		ExecutionMode:               ExecutionModeProduction,
	}

	tests := []struct {
		name     string
		identity JobIdentity
	}{
		{
			name: "empty object ID",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ObjectID = ""
				return invalid
			}(),
		},
		{
			name: "object ID with leading whitespace",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ObjectID = " OBJ-0001"
				return invalid
			}(),
		},
		{
			name: "object ID with trailing whitespace",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ObjectID = "OBJ-0001 "
				return invalid
			}(),
		},
		{
			name: "object ID with NUL",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ObjectID = "OBJ\x000001"
				return invalid
			}(),
		},
		{
			name: "zero light curve revision",
			identity: func() JobIdentity {
				invalid := valid
				invalid.LightCurveRevision = 0
				return invalid
			}(),
		},
		{
			name: "negative light curve revision",
			identity: func() JobIdentity {
				invalid := valid
				invalid.LightCurveRevision = -1
				return invalid
			}(),
		},
		{
			name: "empty model bundle version",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ModelBundleVersion = ""
				return invalid
			}(),
		},
		{
			name: "empty classification policy version",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ClassificationPolicyVersion = ""
				return invalid
			}(),
		},
		{
			name: "unspecified execution mode",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ExecutionMode = ExecutionModeUnspecified
				return invalid
			}(),
		},
		{
			name: "unknown execution mode",
			identity: func() JobIdentity {
				invalid := valid
				invalid.ExecutionMode = ExecutionMode(255)
				return invalid
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := GenerateJobID(tt.identity); err == nil {
				t.Fatal("GenerateJobID() error = nil, want non-nil")
			}
		})
	}
}

func TestGenerateRunIDIsDeterministic(t *testing.T) {
	const jobID = JobID("46709af9-e19b-5dfc-beb5-b9213127fd18")

	first, err := GenerateRunID(jobID)
	if err != nil {
		t.Fatalf("first GenerateRunID() error = %v", err)
	}

	second, err := GenerateRunID(jobID)
	if err != nil {
		t.Fatalf("second GenerateRunID() error = %v", err)
	}

	if first != second {
		t.Fatalf("GenerateRunID() is not deterministic: %q != %q", first, second)
	}
}

func TestGenerateRunIDRejectsInvalidJobID(t *testing.T) {
	tests := []struct {
		name  string
		jobID JobID
	}{
		{name: "empty", jobID: ""},
		{name: "not UUID", jobID: "not-a-uuid"},
		{name: "nil UUID", jobID: "00000000-0000-0000-0000-000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := GenerateRunID(tt.jobID); err == nil {
				t.Fatal("GenerateRunID() error = nil, want non-nil")
			}
		})
	}
}
