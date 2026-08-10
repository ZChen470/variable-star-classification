package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

type lightCurveRevisionResponse struct {
	ObjectID             string                    `json:"object_id"`
	LightCurveRevision   int64                     `json:"light_curve_revision"`
	EligibleEpochCount   uint32                    `json:"eligible_epoch_count"`
	QualityPolicyVersion *string                   `json:"quality_policy_version,omitempty"`
	Epochs               []lightCurveEpochResponse `json:"epochs"`
}

type lightCurveEpochResponse struct {
	ObservationTime float64 `json:"observation_time"`
	Magnitude       float32 `json:"magnitude"`
	MagnitudeError  float32 `json:"magnitude_error"`
}

func newLightCurveHTTPHandler(dataset lightCurveDataset) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /internal/v1/objects/{object_id}/light-curves/{revision}",
		func(writer http.ResponseWriter, request *http.Request) {
			handleGetLightCurveRevision(writer, request, dataset)
		},
	)
	return mux
}

func handleGetLightCurveRevision(writer http.ResponseWriter, request *http.Request, dataset lightCurveDataset) {
	objectID := request.PathValue("object_id")

	revision, err := strconv.ParseInt(request.PathValue("revision"), 10, 64)
	if err != nil || revision <= 0 {
		http.NotFound(writer, request)
		return
	}

	lightCurve, ok := dataset.Revision(objectID, revision)
	if !ok {
		http.NotFound(writer, request)
		return
	}

	payload := mapLightCurveRevisionResponse(lightCurve)

	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(payload); err != nil {
		return
	}
}

func mapLightCurveRevisionResponse(revision domain.LightCurveRevision) lightCurveRevisionResponse {
	epochs := make([]lightCurveEpochResponse, len(revision.Epochs))
	for i, epoch := range revision.Epochs {
		epochs[i] = lightCurveEpochResponse{
			ObservationTime: epoch.ObservationTime,
			Magnitude:       epoch.Magnitude,
			MagnitudeError:  epoch.MagnitudeError,
		}
	}

	var qualityPolicyVersion *string
	if revision.QualityPolicyVersion != nil {
		value := *revision.QualityPolicyVersion
		qualityPolicyVersion = &value
	}

	return lightCurveRevisionResponse{
		ObjectID:             revision.ObjectID,
		LightCurveRevision:   revision.Revision,
		EligibleEpochCount:   revision.EligibleEpochCount,
		QualityPolicyVersion: qualityPolicyVersion,
		Epochs:               epochs,
	}
}
