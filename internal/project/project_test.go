package project

import "testing"

func TestName(t *testing.T) {
	const want = "variable-star-classification"

	if Name != want {
		t.Fatalf("Name = %q, want %q", Name, want)
	}
}
