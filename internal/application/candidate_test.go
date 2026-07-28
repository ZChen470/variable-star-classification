package application

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanentCandidateMessageError(t *testing.T) {
	t.Parallel()

	cause := errors.New("invalid wire data")
	candidateErr := &PermanentCandidateMessageError{
		Code:  CandidateMessageErrorCodeMalformedProto,
		Field: "value",
		Err:   cause,
	}

	const expectedMessage = "CANDIDATE_PROTO_MALFORMED (value): invalid wire data"
	if candidateErr.Error() != expectedMessage {
		t.Fatalf("unexpected error message: got %q, want %q", candidateErr.Error(), expectedMessage)
	}

	wrapped := fmt.Errorf("decode candidate event: %w", candidateErr)

	var permanentErr *PermanentCandidateMessageError
	if !errors.As(wrapped, &permanentErr) {
		t.Fatal("expected wrapped error to contain PermanentCandidateMessageError")
	}
	if permanentErr.Code != CandidateMessageErrorCodeMalformedProto {
		t.Fatalf(
			"unexpected error code: got %q, want %q",
			permanentErr.Code,
			CandidateMessageErrorCodeMalformedProto,
		)
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("expected wrapped error to contain the underlying cause")
	}
}

func TestValidateRequiredCandidateString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       string
		wantMessage string
	}{
		{
			name:  "valid",
			value: "pipeline-v1",
		},
		{
			name:        "empty",
			value:       "",
			wantMessage: "must not be empty",
		},
		{
			name:        "leading whitespace",
			value:       " pipeline-v1",
			wantMessage: "must not contain leading or trailing whitespace",
		},
		{
			name:        "trailing whitespace",
			value:       "pipeline-v1 ",
			wantMessage: "must not contain leading or trailing whitespace",
		},
		{
			name:        "NUL",
			value:       "pipeline\x00-v1",
			wantMessage: "must not contain NUL",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateRequiredCandidateString("test_field", test.value)

			if test.wantMessage == "" {
				if err != nil {
					t.Fatalf("validateRequiredCandidateString returned error: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error %q", test.wantMessage)
			}
			if err.Error() != test.wantMessage {
				t.Fatalf(
					"unexpected error: got %q, want %q",
					err.Error(),
					test.wantMessage,
				)
			}
		})
	}
}
