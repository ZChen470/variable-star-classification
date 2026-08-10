package kafka

import "testing"

func TestNewSCRAMSHA256Mechanism(t *testing.T) {
	t.Parallel()

	mechanism, err := NewSCRAMSHA256Mechanism(" kafka-user ", "secret")
	if err != nil {
		t.Fatalf("NewSCRAMSHA256Mechanism() error = %v", err)
	}

	if got := mechanism.Name(); got != "SCRAM-SHA-256" {
		t.Fatalf("mechanism.Name() = %q, want %q", got, "SCRAM-SHA-256")
	}
}

func TestNewSCRAMSHA256MechanismRejectsBlankCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "blank username",
			username: " ",
			password: "secret",
		},
		{
			name:     "blank password",
			username: "kafka-user",
			password: " ",
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := NewSCRAMSHA256Mechanism(test.username, test.password); err == nil {
				t.Fatal("NewSCRAMSHA256Mechanism() error = nil")
			}
		})
	}
}
