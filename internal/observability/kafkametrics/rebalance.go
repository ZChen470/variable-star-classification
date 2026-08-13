package kafkametrics

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RebalanceObserver exposes low-cardinality observability for consumer-group
// rebalances blocked by BlockRebalanceOnPoll.
type RebalanceObserver struct {
	callbackBlocked prometheus.Counter
}

func NewRebalanceObserver(registerer prometheus.Registerer) (*RebalanceObserver, error) {
	if registerer == nil {
		return nil, errors.New("Kafka rebalance metrics registerer must not be nil")
	}

	observer := &RebalanceObserver{
		callbackBlocked: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "kafka",
				Name:      "rebalance_callback_blocked_total",
				Help:      "Number of consumer-group partition callbacks blocked by BlockRebalanceOnPoll.",
			},
		),
	}

	if err := registerer.Register(observer.callbackBlocked); err != nil {
		return nil, fmt.Errorf("register Kafka rebalance metric: %w", err)
	}

	return observer, nil
}

// OnPartitionsCallbackBlocked records franz-go's signal that a partition
// assignment, revocation, or loss callback is blocked by BlockRebalanceOnPoll.
func (observer *RebalanceObserver) OnPartitionsCallbackBlocked(_ context.Context, _ *kgo.Client) {
	if observer == nil {
		return
	}

	observer.callbackBlocked.Inc()
}
