package logging

import (
	"io"
	"log/slog"
)

// NewJSON creates the process logger used by runtime entrypoints.
//
// The service attribute is attached once at logger construction time so every
// record emitted by the process can be attributed without repeating it at each
// call site.
func NewJSON(writer io.Writer, service string) *slog.Logger {
	handler := slog.NewJSONHandler(
		writer,
		&slog.HandlerOptions{
			Level: slog.LevelInfo,
		},
	)

	return slog.New(handler).With(
		slog.String("service", service),
	)
}
