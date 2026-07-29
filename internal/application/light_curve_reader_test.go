package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakelightcurve"
)

func TestLightCurveRevisionReaderForwardsExactRequest(t *testing.T) {
	request := fakelightcurve.Request{
		ObjectID: " OBJ-0001 ",
		Revision: 21,
	}

	qualityPolicyVersion := "quality-policy-v1"
	want := domain.LightCurveRevision{
		ObjectID:             request.ObjectID,
		Revision:             request.Revision,
		EligibleEpochCount:   3,
		QualityPolicyVersion: &qualityPolicyVersion,
		Epochs: []domain.LightCurveEpoch{
			{
				ObservationTime: 60421.123456,
				Magnitude:       17.31,
				MagnitudeError:  0.03,
			},
			{
				ObservationTime: 60422.123456,
				Magnitude:       17.42,
				MagnitudeError:  0.04,
			},
			{
				ObservationTime: 60423.123456,
				Magnitude:       17.53,
				MagnitudeError:  0.05,
			},
		},
	}

	repository := fakelightcurve.New(
		map[fakelightcurve.Request]fakelightcurve.Response{
			request: {
				Revision: want,
			},
		},
	)

	reader, err := application.NewLightCurveRevisionReader(repository)
	if err != nil {
		t.Fatalf("NewLightCurveRevisionReader() error = %v", err)
	}

	got, err := reader.ReadRevision(
		context.Background(),
		request.ObjectID,
		request.Revision,
	)
	if err != nil {
		t.Fatalf("ReadRevision() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadRevision() = %#v, want %#v", got, want)
	}

	calls := repository.Calls()
	wantCalls := []fakelightcurve.Request{request}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("repository calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestLightCurveRevisionReaderRejectsIdentityMismatch(t *testing.T) {
	request := fakelightcurve.Request{
		ObjectID: "OBJ-0003",
		Revision: 9,
	}

	tests := []struct {
		name     string
		returned domain.LightCurveRevision
	}{
		{
			name: "object id mismatch",
			returned: domain.LightCurveRevision{
				ObjectID: "OBJ-OTHER",
				Revision: request.Revision,
			},
		},
		{
			name: "revision mismatch",
			returned: domain.LightCurveRevision{
				ObjectID: request.ObjectID,
				Revision: request.Revision + 1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := fakelightcurve.New(
				map[fakelightcurve.Request]fakelightcurve.Response{
					request: {
						Revision: test.returned,
					},
				},
			)

			reader, err := application.NewLightCurveRevisionReader(
				repository,
			)
			if err != nil {
				t.Fatalf(
					"NewLightCurveRevisionReader() error = %v",
					err,
				)
			}

			got, err := reader.ReadRevision(
				context.Background(),
				request.ObjectID,
				request.Revision,
			)
			if !errors.Is(
				err,
				application.ErrLightCurveRevisionIdentityMismatch,
			) {
				t.Fatalf(
					"ReadRevision() error = %v, want %v",
					err,
					application.ErrLightCurveRevisionIdentityMismatch,
				)
			}
			if !reflect.DeepEqual(
				got,
				domain.LightCurveRevision{},
			) {
				t.Fatalf(
					"ReadRevision() = %#v, want zero value",
					got,
				)
			}
		})
	}
}

func TestLightCurveRevisionReaderPreservesRepositoryErrors(t *testing.T) {
	request := fakelightcurve.Request{
		ObjectID: "OBJ-0004",
		Revision: 10,
	}

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "not found",
			err:  application.ErrLightCurveRevisionNotFound,
		},
		{
			name: "not ready",
			err:  application.ErrLightCurveRevisionNotReady,
		},
		{
			name: "inconsistent",
			err:  application.ErrLightCurveRevisionInconsistent,
		},
		{
			name: "source unavailable",
			err:  application.ErrLightCurveSourceUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := fakelightcurve.New(
				map[fakelightcurve.Request]fakelightcurve.Response{
					request: {
						Err: test.err,
					},
				},
			)

			reader, err := application.NewLightCurveRevisionReader(
				repository,
			)
			if err != nil {
				t.Fatalf(
					"NewLightCurveRevisionReader() error = %v",
					err,
				)
			}

			_, err = reader.ReadRevision(
				context.Background(),
				request.ObjectID,
				request.Revision,
			)
			if !errors.Is(err, test.err) {
				t.Fatalf(
					"ReadRevision() error = %v, want %v",
					err,
					test.err,
				)
			}

			calls := repository.Calls()
			wantCalls := []fakelightcurve.Request{request}
			if !reflect.DeepEqual(calls, wantCalls) {
				t.Fatalf(
					"repository calls = %#v, want %#v",
					calls,
					wantCalls,
				)
			}
		})
	}
}

func TestLightCurveRevisionReaderPropagatesCancelledContext(t *testing.T) {
	repository := fakelightcurve.New(nil)

	reader, err := application.NewLightCurveRevisionReader(repository)
	if err != nil {
		t.Fatalf("NewLightCurveRevisionReader() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = reader.ReadRevision(ctx, "OBJ-0005", 11)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"ReadRevision() error = %v, want context.Canceled",
			err,
		)
	}
	if gotCalls := len(repository.Calls()); gotCalls != 0 {
		t.Fatalf("repository call count = %d, want 0", gotCalls)
	}
}

func TestLightCurveRevisionReaderRejectsNilDependencies(t *testing.T) {
	reader, err := application.NewLightCurveRevisionReader(nil)
	if err == nil {
		t.Fatal("NewLightCurveRevisionReader() error = nil, want error")
	}
	if reader != nil {
		t.Fatalf(
			"NewLightCurveRevisionReader() = %#v, want nil",
			reader,
		)
	}

	repository := fakelightcurve.New(nil)
	reader, err = application.NewLightCurveRevisionReader(repository)
	if err != nil {
		t.Fatalf("NewLightCurveRevisionReader() error = %v", err)
	}

	_, err = reader.ReadRevision(nil, "OBJ-0006", 12)
	if err == nil {
		t.Fatal("ReadRevision() nil context error = nil, want error")
	}
	if gotCalls := len(repository.Calls()); gotCalls != 0 {
		t.Fatalf("repository call count = %d, want 0", gotCalls)
	}

	var nilReader *application.LightCurveRevisionReader
	_, err = nilReader.ReadRevision(
		context.Background(),
		"OBJ-0007",
		13,
	)
	if err == nil {
		t.Fatal("nil reader ReadRevision() error = nil, want error")
	}
}
