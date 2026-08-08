package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewJSONWritesStructuredRecord(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer

	logger := NewJSON(&output, "candidate-orchestrator")
	logger.Info(
		"service started",
		"operation", "startup",
	)

	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatalf("decode log record: %v", err)
	}

	if record["level"] != "INFO" {
		t.Fatalf("level = %#v, want INFO", record["level"])
	}

	if record["msg"] != "service started" {
		t.Fatalf(
			"msg = %#v, want %q",
			record["msg"],
			"service started",
		)
	}

	if record["service"] != "candidate-orchestrator" {
		t.Fatalf(
			"service = %#v, want %q",
			record["service"],
			"candidate-orchestrator",
		)
	}

	if record["operation"] != "startup" {
		t.Fatalf(
			"operation = %#v, want %q",
			record["operation"],
			"startup",
		)
	}

	if _, ok := record["time"]; !ok {
		t.Fatal("log record does not contain time")
	}
}
