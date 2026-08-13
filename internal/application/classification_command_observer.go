package application

// ClassificationCommandObserver observes runtime processing events without
// coupling the application layer to a concrete metrics implementation.
//
// Implementations must not change command processing behavior.
type ClassificationCommandObserver interface {
	RetryStarted()
	RetryAttempted()
	RetryFinished()
	DLQPublished()
}

type noopClassificationCommandObserver struct{}

func (noopClassificationCommandObserver) RetryStarted()   {}
func (noopClassificationCommandObserver) RetryAttempted() {}
func (noopClassificationCommandObserver) RetryFinished()  {}
func (noopClassificationCommandObserver) DLQPublished()   {}

func classificationCommandObserverOrNoop(observer ClassificationCommandObserver) ClassificationCommandObserver {
	if observer == nil {
		return noopClassificationCommandObserver{}
	}

	return observer
}
