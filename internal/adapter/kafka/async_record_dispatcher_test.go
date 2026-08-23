package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeMessageSubmitter struct {
	message application.InboundMessage

	callback func(error)

	err error
}

func (submitter *fakeMessageSubmitter) Submit(
	_ context.Context,
	message application.InboundMessage,
	completion func(error),
) error {

	if submitter.err != nil {
		return submitter.err
	}

	submitter.message = message
	submitter.callback = completion

	return nil
}

func TestAsyncRecordDispatcherDispatchesCompletion(
	t *testing.T,
) {
	t.Parallel()

	completionCh :=
		make(
			chan RecordCompletion,
			1,
		)

	submitter :=
		&fakeMessageSubmitter{}

	dispatcher, err :=
		newAsyncRecordDispatcher(
			submitter,
			completionCh,
		)

	if err != nil {
		t.Fatal(err)
	}

	record :=
		&kgo.Record{
			Topic:     "topic",
			Partition: 2,
			Offset:    100,
			Key: []byte{
				'a',
			},
			Value: []byte{
				'b',
			},
		}

	err =
		dispatcher.Dispatch(
			context.Background(),
			record,
		)

	if err != nil {
		t.Fatal(err)
	}

	if submitter.message.Topic != "topic" {
		t.Fatalf(
			"topic=%q want topic",
			submitter.message.Topic,
		)
	}

	if submitter.callback == nil {
		t.Fatal(
			"completion callback is nil",
		)
	}

	submitter.callback(nil)

	completion :=
		<-completionCh

	if completion.record != record {
		t.Fatal(
			"completion record mismatch",
		)
	}

	if completion.err != nil {
		t.Fatalf(
			"completion error=%v",
			completion.err,
		)
	}
}

func TestAsyncRecordDispatcherPropagatesSubmitFailure(
	t *testing.T,
) {
	t.Parallel()

	submitter :=
		&fakeMessageSubmitter{
			err: errors.New(
				"submit failed",
			),
		}

	dispatcher, err :=
		newAsyncRecordDispatcher(
			submitter,
			make(chan RecordCompletion, 1),
		)

	if err != nil {
		t.Fatal(err)
	}

	err =
		dispatcher.Dispatch(
			context.Background(),
			&kgo.Record{
				Topic: "topic",
			},
		)

	if err == nil {
		t.Fatal(
			"expected submit error",
		)
	}
}

func TestAsyncRecordDispatcherRejectsNilRecord(
	t *testing.T,
) {
	t.Parallel()

	dispatcher, err :=
		newAsyncRecordDispatcher(
			&fakeMessageSubmitter{},
			make(chan RecordCompletion, 1),
		)

	if err != nil {
		t.Fatal(err)
	}

	err =
		dispatcher.Dispatch(
			context.Background(),
			nil,
		)

	if err == nil {
		t.Fatal(
			"expected nil record error",
		)
	}
}
