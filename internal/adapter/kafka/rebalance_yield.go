package kafka

import (
	"context"
	"sync"
)

// RebalanceYield coordinates a franz-go blocked-rebalance signal with the
// currently processed Kafka record or bounded record batch.
//
// Request increments the rebalance generation and cancels the active processing
// context, if any. A runner can compare the generation captured before poll
// with the current generation to determine whether it must yield ownership.
type RebalanceYield struct {
	mu         sync.Mutex
	generation uint64
	cancel     context.CancelFunc
}

func NewRebalanceYield() *RebalanceYield {
	return &RebalanceYield{}
}

// Generation returns the current rebalance-yield generation.
func (yield *RebalanceYield) Generation() uint64 {
	if yield == nil {
		return 0
	}

	yield.mu.Lock()
	defer yield.mu.Unlock()

	return yield.generation
}

// Request signals that the current BlockRebalanceOnPoll processing window
// should end as soon as possible.
func (yield *RebalanceYield) Request() {
	if yield == nil {
		return
	}

	yield.mu.Lock()
	yield.generation++
	cancel := yield.cancel
	yield.mu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// Bind derives a context for one in-flight Kafka record or bounded batch.
//
// baseline must be the generation captured before polling that record. If a
// yield request arrived after baseline was captured, the returned context is
// cancelled immediately.
func (yield *RebalanceYield) Bind(
	parent context.Context,
	baseline uint64,
) (context.Context, func()) {
	processingContext, cancel := context.WithCancel(parent)

	if yield == nil {
		return processingContext, cancel
	}

	yield.mu.Lock()
	yield.cancel = cancel
	requested := yield.generation != baseline
	yield.mu.Unlock()

	if requested {
		cancel()
	}

	release := func() {
		yield.mu.Lock()
		yield.cancel = nil
		yield.mu.Unlock()

		cancel()
	}

	return processingContext, release
}

// RequestedSince reports whether a rebalance yield was requested after the
// supplied generation was captured.
func (yield *RebalanceYield) RequestedSince(generation uint64) bool {
	if yield == nil {
		return false
	}

	yield.mu.Lock()
	defer yield.mu.Unlock()

	return yield.generation != generation
}
