package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	lightcurveadapter "github.com/ZChen470/variable-star-classification/internal/adapter/lightcurve"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestLightCurveHTTPHandlerMatchesRepositoryContract(t *testing.T) {
	t.Parallel()

	dataset, expected := testLightCurveDataset()

	server := httptest.NewServer(newLightCurveHTTPHandler(dataset))
	defer server.Close()

	repository, err := lightcurveadapter.NewRepository(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	got, err := repository.GetRevision(context.Background(), expected.ObjectID, expected.Revision)
	if err != nil {
		t.Fatalf("GetRevision() error = %v", err)
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("GetRevision() = %#v, want %#v", got, expected)
	}
}

func TestLightCurveHTTPHandlerReturnsNotFoundForUnknownObject(t *testing.T) {
	t.Parallel()

	dataset, _ := testLightCurveDataset()

	server := httptest.NewServer(newLightCurveHTTPHandler(dataset))
	defer server.Close()

	repository, err := lightcurveadapter.NewRepository(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	_, err = repository.GetRevision(context.Background(), "missing-object", mockLightCurveRevision)
	if !errors.Is(err, application.ErrLightCurveRevisionNotFound) {
		t.Fatalf("GetRevision() error = %v, want ErrLightCurveRevisionNotFound", err)
	}
}

func TestLightCurveHTTPHandlerReturnsNotFoundForUnknownRevision(t *testing.T) {
	t.Parallel()

	dataset, expected := testLightCurveDataset()

	server := httptest.NewServer(newLightCurveHTTPHandler(dataset))
	defer server.Close()

	repository, err := lightcurveadapter.NewRepository(server.URL, server.Client())
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	_, err = repository.GetRevision(context.Background(), expected.ObjectID, expected.Revision+1)
	if !errors.Is(err, application.ErrLightCurveRevisionNotFound) {
		t.Fatalf("GetRevision() error = %v, want ErrLightCurveRevisionNotFound", err)
	}
}

func TestLightCurveHTTPHandlerRejectsUnsupportedMethod(t *testing.T) {
	t.Parallel()

	dataset, expected := testLightCurveDataset()

	server := httptest.NewServer(newLightCurveHTTPHandler(dataset))
	defer server.Close()

	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/internal/v1/objects/"+expected.ObjectID+"/light-curves/1",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusMethodNotAllowed)
	}
}

func testLightCurveDataset() (lightCurveDataset, domain.LightCurveRevision) {
	qualityPolicyVersion := mockQualityPolicyVersion

	revision := domain.LightCurveRevision{
		ObjectID:             "object-a",
		Revision:             mockLightCurveRevision,
		EligibleEpochCount:   3,
		QualityPolicyVersion: &qualityPolicyVersion,
		Epochs: []domain.LightCurveEpoch{
			{
				ObservationTime: 59001,
				Magnitude:       15.1,
				MagnitudeError:  0.01,
			},
			{
				ObservationTime: 59002,
				Magnitude:       15.2,
				MagnitudeError:  0.02,
			},
			{
				ObservationTime: 59003,
				Magnitude:       15.3,
				MagnitudeError:  0.03,
			},
		},
	}

	return lightCurveDataset{
		objectIDs: []string{revision.ObjectID},
		revisions: map[string]domain.LightCurveRevision{
			revision.ObjectID: revision,
		},
	}, revision
}
