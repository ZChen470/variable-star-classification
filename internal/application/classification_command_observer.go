package application

import "time"

// ClassificationCommandObserver observes runtime processing events without
// coupling the application layer to a concrete metrics implementation.
//
// Implementations must not change command processing behavior.
type ClassificationCommandObserver interface {
	RetryStarted()
	RetryAttempted()
	RetryFinished()
	DLQPublished()
	CommandStarted()
	CommandFinished(duration time.Duration)
}

type noopClassificationCommandObserver struct{}

func (noopClassificationCommandObserver) RetryStarted()                          {}
func (noopClassificationCommandObserver) RetryAttempted()                        {}
func (noopClassificationCommandObserver) RetryFinished()                         {}
func (noopClassificationCommandObserver) DLQPublished()                          {}
func (noopClassificationCommandObserver) CommandStarted()                        {}
func (noopClassificationCommandObserver) CommandFinished(duration time.Duration) {}

func classificationCommandObserverOrNoop(observer ClassificationCommandObserver) ClassificationCommandObserver {
	if observer == nil {
		return noopClassificationCommandObserver{}
	}

	return observer
}
