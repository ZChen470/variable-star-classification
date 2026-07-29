package fakelightcurve

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestRepositoryReturnsConfiguredRevisionAndRecordsRequest(t *testing.T) {
	request := Request{
		ObjectID: " OBJ-0001 ",
		Revision: 21,
	}

	qualityPolicyVersion := "quality-policy-v1"
	configuredRevision := domain.LightCurveRevision{
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

	repository := New(map[Request]Response{
		request: {
			Revision: configuredRevision,
		},
	})

	// 修改传入 New 的原始数据，不应影响 Fake 内部保存的响应。
	qualityPolicyVersion = "modified-after-new"
	configuredRevision.Epochs[0].Magnitude = 99

	got, err := repository.GetRevision(
		context.Background(),
		request.ObjectID,
		request.Revision,
	)
	if err != nil {
		t.Fatalf("GetRevision() error = %v", err)
	}

	wantQualityPolicyVersion := "quality-policy-v1"
	want := domain.LightCurveRevision{
		ObjectID:             request.ObjectID,
		Revision:             request.Revision,
		EligibleEpochCount:   3,
		QualityPolicyVersion: &wantQualityPolicyVersion,
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

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetRevision() = %#v, want %#v", got, want)
	}

	calls := repository.Calls()
	if len(calls) != 1 {
		t.Fatalf("call count = %d, want 1", len(calls))
	}
	if calls[0] != request {
		t.Fatalf("recorded request = %#v, want %#v", calls[0], request)
	}

	// 修改第一次返回的数据，不应影响后续读取结果。
	got.Epochs[0].Magnitude = 88
	*got.QualityPolicyVersion = "modified-return"

	gotAgain, err := repository.GetRevision(
		context.Background(),
		request.ObjectID,
		request.Revision,
	)
	if err != nil {
		t.Fatalf("second GetRevision() error = %v", err)
	}
	if !reflect.DeepEqual(gotAgain, want) {
		t.Fatalf(
			"second GetRevision() = %#v, want %#v",
			gotAgain,
			want,
		)
	}
}

func TestRepositoryReturnsConfiguredErrorAndRecordsRequest(t *testing.T) {
	request := Request{
		ObjectID: "OBJ-0002",
		Revision: 7,
	}
	wantErr := application.ErrLightCurveSourceUnavailable

	repository := New(map[Request]Response{
		request: {
			Err: wantErr,
		},
	})

	got, err := repository.GetRevision(
		context.Background(),
		request.ObjectID,
		request.Revision,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("GetRevision() error = %v, want %v", err, wantErr)
	}
	if !reflect.DeepEqual(got, domain.LightCurveRevision{}) {
		t.Fatalf("GetRevision() revision = %#v, want zero value", got)
	}

	calls := repository.Calls()
	if len(calls) != 1 {
		t.Fatalf("call count = %d, want 1", len(calls))
	}
	if calls[0] != request {
		t.Fatalf("recorded request = %#v, want %#v", calls[0], request)
	}
}

func TestRepositoryRejectsUnconfiguredRequest(t *testing.T) {
	repository := New(nil)

	_, err := repository.GetRevision(
		context.Background(),
		"OBJ-UNCONFIGURED",
		31,
	)
	if !errors.Is(err, ErrUnconfiguredRequest) {
		t.Fatalf(
			"GetRevision() error = %v, want %v",
			err,
			ErrUnconfiguredRequest,
		)
	}

	calls := repository.Calls()
	if len(calls) != 1 {
		t.Fatalf("call count = %d, want 1", len(calls))
	}
}

func TestRepositoryRejectsCancelledContextWithoutRecordingCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repository := New(nil)

	_, err := repository.GetRevision(ctx, "OBJ-0003", 9)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetRevision() error = %v, want context.Canceled", err)
	}

	if gotCalls := len(repository.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}

func TestRepositoryRejectsNilContext(t *testing.T) {
	repository := New(nil)

	_, err := repository.GetRevision(nil, "OBJ-0004", 10)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf(
			"GetRevision() error = %v, want %v",
			err,
			ErrNilContext,
		)
	}

	if gotCalls := len(repository.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}
