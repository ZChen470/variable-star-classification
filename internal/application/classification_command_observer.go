package application

// ClassificationCommandObserver observes runtime processing events without
// coupling the application layer to a concrete metrics implementation.
//
// Implementations must not change command processing behavior.
type ClassificationCommandObserver interface {
	RetryAttempted()
	RetryExhausted()
	DLQPublished()
}

type noopClassificationCommandObserver struct{}

func (noopClassificationCommandObserver) RetryAttempted() {}

func (noopClassificationCommandObserver) RetryExhausted() {}

func (noopClassificationCommandObserver) DLQPublished() {}

func classificationCommandObserverOrNoop(
	observer ClassificationCommandObserver,
) ClassificationCommandObserver {
	if observer == nil {
		return noopClassificationCommandObserver{}
	}

	return observer
}
