package kafka

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
	"time"
)

var ErrCommitManagerStopped = errors.New("commit manager stopped")

const defaultCommitQueueCapacity = 10000

type commitManager struct {
	batcher *commitBatcher

	maxBatchSize  int
	flushInterval time.Duration

	pendingCount int

	addCh chan *kgo.Record

	flushRequestCh chan struct{}

	stopCh chan struct{}
	doneCh chan struct{}

	stopOnce sync.Once
}

func newCommitManager(
	batcher *commitBatcher,
	maxBatchSize int,
	flushInterval time.Duration,
) (*commitManager, error) {

	if batcher == nil {
		return nil, errors.New(
			"create commit manager: nil batcher",
		)
	}

	if maxBatchSize < 1 {
		return nil, errors.New(
			"create commit manager: invalid batch size",
		)
	}

	if flushInterval <= 0 {
		return nil, errors.New(
			"create commit manager: invalid flush interval",
		)
	}

	return &commitManager{
		batcher: batcher,

		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,

		addCh: make(
			chan *kgo.Record,
			defaultCommitQueueCapacity,
		),

		flushRequestCh: make(
			chan struct{},
			1,
		),

		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}, nil
}

func (manager *commitManager) Start(ctx context.Context) {
	go manager.run(ctx)
}

func (manager *commitManager) AddWatermark(record *kgo.Record) error {

	if record == nil {
		return errors.New(
			"add commit watermark: nil record",
		)
	}

	select {
	case manager.addCh <- record:
		return nil

	case <-manager.stopCh:
		return ErrCommitManagerStopped
	}
}

func (manager *commitManager) run(ctx context.Context) {
	defer close(manager.doneCh)

	ticker := time.NewTicker(manager.flushInterval)
	defer ticker.Stop()

	for {
		select {

		case record := <-manager.addCh:
			if record == nil {
				continue
			}
			if err := manager.batcher.Add(record); err != nil {
				continue
			}
			manager.pendingCount++

			if manager.pendingCount >= manager.maxBatchSize {
				manager.RequestFlush()
			}

		case <-manager.flushRequestCh:
			if manager.batcher.Flush(ctx) == nil {
				manager.pendingCount = 0
			}

		case <-ticker.C:
			if manager.batcher.Flush(ctx) == nil {
				manager.pendingCount = 0
			}

		case <-manager.stopCh: // 排空 addCh 并处理 batcher
			for {
				select {
				case record := <-manager.addCh:
					if record == nil {
						continue
					}
					if err := manager.batcher.Add(record); err != nil {
						continue
					}
				default:
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

					_ = manager.batcher.Flush(shutdownCtx)

					cancel()
					return
				}
			}

		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			_ = manager.batcher.Flush(shutdownCtx)

			cancel()

			return
		}
	}
}

func (manager *commitManager) RequestFlush() {
	select {
	case manager.flushRequestCh <- struct{}{}:
	default:
	}
}

func (manager *commitManager) Stop(ctx context.Context) error {
	manager.stopOnce.Do(
		func() {
			close(manager.stopCh)
		},
	)

	select {
	case <-manager.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
