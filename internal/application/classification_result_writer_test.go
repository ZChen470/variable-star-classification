package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestClassificationResultWriterHandlerTreatsSaveOutcomesAsSuccess(
	t *testing.T,
) {
	tests := []struct {
		name   string
		result application.SaveRunResult
	}{
		{
			name: "run inserted and current advanced",
			result: application.SaveRunResult{
				RunInserted:     true,
				CurrentAdvanced: true,
			},
		},
		{
			name: "run inserted without current advance",
			result: application.SaveRunResult{
				RunInserted:     true,
				CurrentAdvanced: false,
			},
		},
		{
			name: "idempotent duplicate",
			result: application.SaveRunResult{
				RunInserted:     false,
				CurrentAdvanced: false,
			},
		},
		{
			name: "repository defined normal outcome",
			result: application.SaveRunResult{
				RunInserted:     false,
				CurrentAdvanced: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := validClassificationResultDecodeRun(t)

			message :=
				buildClassificationResultDecodeMessage(
					t,
					run,
					application.TraceContext{},
				)

			repository :=
				&classificationResultWriterTestRepository{
					result: test.result,
				}

			handler, err :=
				application.
					NewClassificationResultWriterHandler(
						classificationResultDecodeTopic,
						repository,
					)
			if err != nil {
				t.Fatalf(
					"NewClassificationResultWriterHandler() error = %v",
					err,
				)
			}

			if err := handler.Handle(
				context.Background(),
				message,
			); err != nil {
				t.Fatalf(
					"Handle() error = %v",
					err,
				)
			}

			if len(repository.runs) != 1 {
				t.Fatalf(
					"repository call count = %d, want 1",
					len(repository.runs),
				)
			}

			wantRun := run
			wantRun.CompletedAt =
				run.CompletedAt.UTC()

			if !reflect.DeepEqual(
				repository.runs[0],
				wantRun,
			) {
				t.Fatalf(
					"saved run mismatch:\ngot  = %#v\nwant = %#v",
					repository.runs[0],
					wantRun,
				)
			}
		})
	}
}

func TestClassificationResultWriterHandlerStopsOnDecodeError(
	t *testing.T,
) {
	repository :=
		&classificationResultWriterTestRepository{}

	handler, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultDecodeTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{
			Topic: classificationResultDecodeTopic,

			Key:   []byte("OBJ-INVALID"),
			Value: []byte{0xff},
		},
	)

	var permanentError *application.
		PermanentClassificationResultError

	if !errors.As(got, &permanentError) {
		t.Fatalf(
			"Handle() error = %v, want PermanentClassificationResultError",
			got,
		)
	}

	if permanentError.Code !=
		application.
			ClassificationResultErrorCodeMalformedMessage {
		t.Fatalf(
			"Code = %q, want %q",
			permanentError.Code,
			application.
				ClassificationResultErrorCodeMalformedMessage,
		)
	}

	if len(repository.runs) != 0 {
		t.Fatalf(
			"repository call count = %d, want 0",
			len(repository.runs),
		)
	}
}

func TestClassificationResultWriterHandlerReturnsTransientRepositoryError(
	t *testing.T,
) {
	run := validClassificationResultDecodeRun(t)

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		application.TraceContext{},
	)

	repositoryCause := errors.New(
		"PostgreSQL temporarily unavailable",
	)

	repository :=
		&classificationResultWriterTestRepository{
			err: repositoryCause,
		}

	handler, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultDecodeTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	got := handler.Handle(
		context.Background(),
		message,
	)

	if !errors.Is(got, repositoryCause) {
		t.Fatalf(
			"errors.Is(error, repositoryCause) = false; error=%v",
			got,
		)
	}

	var permanentError *application.
		PermanentClassificationResultError

	if errors.As(got, &permanentError) {
		t.Fatalf(
			"transient repository error was classified as permanent: %v",
			got,
		)
	}

	if len(repository.runs) != 1 {
		t.Fatalf(
			"repository call count = %d, want 1",
			len(repository.runs),
		)
	}
}

func TestClassificationResultWriterHandlerClassifiesRepositoryConflict(
	t *testing.T,
) {
	run := validClassificationResultDecodeRun(t)

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		application.TraceContext{},
	)

	repository :=
		&classificationResultWriterTestRepository{
			err: application.ErrClassificationRunConflict,
		}

	handler, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultDecodeTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	got := handler.Handle(
		context.Background(),
		message,
	)

	var permanentError *application.
		PermanentClassificationResultError

	if !errors.As(got, &permanentError) {
		t.Fatalf(
			"Handle() error = %v, want PermanentClassificationResultError",
			got,
		)
	}

	if permanentError.Code !=
		application.
			ClassificationResultErrorCodeRepositoryConflict {
		t.Fatalf(
			"Code = %q, want %q",
			permanentError.Code,
			application.
				ClassificationResultErrorCodeRepositoryConflict,
		)
	}

	if permanentError.Field != "classification_run" {
		t.Fatalf(
			"Field = %q, want classification_run",
			permanentError.Field,
		)
	}

	if !errors.Is(
		got,
		application.ErrClassificationRunConflict,
	) {
		t.Fatalf(
			"errors.Is(error, ErrClassificationRunConflict) = false",
		)
	}
}

func TestClassificationResultWriterHandlerReturnsCancelledContext(
	t *testing.T,
) {
	run := validClassificationResultDecodeRun(t)

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		application.TraceContext{},
	)

	repository :=
		&classificationResultWriterTestRepository{}

	handler, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultDecodeTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	got := handler.Handle(ctx, message)

	if !errors.Is(got, context.Canceled) {
		t.Fatalf(
			"Handle() error = %v, want context.Canceled",
			got,
		)
	}

	if len(repository.runs) != 0 {
		t.Fatalf(
			"repository call count = %d, want 0",
			len(repository.runs),
		)
	}
}

func TestNewClassificationResultWriterHandlerRejectsInvalidArguments(
	t *testing.T,
) {
	validRepository :=
		&classificationResultWriterTestRepository{}

	tests := []struct {
		name        string
		resultTopic string
		repository  application.ClassificationRunSaver
	}{
		{
			name:        "empty result topic",
			resultTopic: "",
			repository:  validRepository,
		},
		{
			name:        "nil repository",
			resultTopic: classificationResultDecodeTopic,
			repository:  nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err :=
				application.
					NewClassificationResultWriterHandler(
						test.resultTopic,
						test.repository,
					)

			if err == nil {
				t.Fatalf(
					"error = nil, handler = %#v",
					handler,
				)
			}
		})
	}
}

type classificationResultWriterTestRepository struct {
	result application.SaveRunResult
	err    error

	runs []domain.ClassificationRun
}

func (
	repository *classificationResultWriterTestRepository,
) SaveRunAndMaybeAdvanceCurrent(
	_ context.Context,
	run domain.ClassificationRun,
) (application.SaveRunResult, error) {
	repository.runs = append(
		repository.runs,
		run,
	)

	return repository.result, repository.err
}
