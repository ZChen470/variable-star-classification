package kafka

import (
	"context"
	"errors"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type asyncRecordDispatcher struct {
	submitter application.MessageSubmitter
	output    chan<- RecordCompletion
}

func newAsyncRecordDispatcher(
	submitter application.MessageSubmitter,
	output chan<- RecordCompletion,
) (*asyncRecordDispatcher, error) {

	if submitter == nil {
		return nil, errors.New(
			"create async record dispatcher: nil submitter",
		)
	}

	if output == nil {
		return nil, errors.New(
			"create async record dispatcher: nil output",
		)
	}

	return &asyncRecordDispatcher{
		submitter: submitter,
		output:    output,
	}, nil
}

func (dispatcher *asyncRecordDispatcher) Dispatch(ctx context.Context, record *kgo.Record) error {
	if record == nil {
		return errors.New("dispatch record: nil record")
	}

	return dispatcher.submitter.Submit(ctx, inboundMessageFromRecord(record), func(err error) {
		completion := RecordCompletion{
			record: record,
			err:    err,
		}

		select {
		case dispatcher.output <- completion:
		case <-ctx.Done():
		}
	})
}
