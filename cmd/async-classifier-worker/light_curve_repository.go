package main

import (
	"net/http"

	lightcurveadapter "github.com/ZChen470/variable-star-classification/internal/adapter/lightcurve"
	"github.com/ZChen470/variable-star-classification/internal/application"
)

func newLightCurveRepository(
	baseURL string,
	httpClient *http.Client,
) (application.LightCurveRepository, error) {
	return lightcurveadapter.NewRepository(
		baseURL,
		httpClient,
	)
}
