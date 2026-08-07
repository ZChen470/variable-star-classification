package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

type stubScienceClassifier struct {
	output application.ClassificationOutput
	err    error

	calls int
	input application.ClassificationInput
}

func (
	classifier *stubScienceClassifier,
) Classify(
	_ context.Context,
	input application.ClassificationInput,
) (application.ClassificationOutput, error) {
	classifier.calls++
	classifier.input = input

	return classifier.output,
		classifier.err
}

func TestScienceServerClassifyUpload(
	t *testing.T,
) {
	classifier :=
		&stubScienceClassifier{
			output: application.ClassificationOutput{
				CoarseProbabilities: [7]float32{
					0.10,
					0.05,
					0.15,
					0.10,
					0.20,
					0.30,
					0.10,
				},

				ConditionalFineProbabilities: [10]float32{
					0.6,
					0.4,
					0.7,
					0.3,
					0.8,
					0.2,
					0.55,
					0.45,
					0.9,
					0.1,
				},

				LeafProbabilities: [12]float32{
					0.09,
					0.06,
					0.07,
					0.03,
					0.24,
					0.06,
					0.055,
					0.045,
					0.18,
					0.02,
					0.05,
					0.10,
				},

				XGBoostExecuted: true,
			},
		}

	servingBundle :=
		application.ServingBundleMetadata{
			ModelBundleVersion: "test-bundle",

			Entrypoint: application.ServingEntrypointMetadata{
				ModelName: "variable_star_classifier",

				ModelVersion: "1",
			},

			CoarseProbabilityOrder: [7]string{
				"ROTATING",
				"CATACLYSMIC",
				"ECLIPSING_BINARY",
				"LONG_PERIOD",
				"PULSATING",
				"RR_LYRAE",
				"SUPERNOVA",
			},

			ConditionalFineProbabilityOrder: [10]string{
				"EW",
				"EA",
				"BY_DRA",
				"RS_CVN",
				"RRAB",
				"RRC",
				"SR",
				"MIRA",
				"DSCT",
				"CEP",
			},

			LeafProbabilityOrder: [12]string{
				"EW",
				"EA",
				"BY_DRA",
				"RS_CVN",
				"RRAB",
				"RRC",
				"SR",
				"MIRA",
				"DSCT",
				"CEP",
				"CATACLYSMIC",
				"SUPERNOVA",
			},
		}

	server, err :=
		newScienceServer(
			classifier,
			servingBundle,
		)
	if err != nil {
		t.Fatalf(
			"newScienceServer() error = %v",
			err,
		)
	}

	var body bytes.Buffer

	writer :=
		multipart.NewWriter(
			&body,
		)

	part, err :=
		writer.CreateFormFile(
			"file",
			"curve.csv",
		)
	if err != nil {
		t.Fatalf(
			"CreateFormFile() error = %v",
			err,
		)
	}

	_, err = part.Write(
		[]byte(
			`time,magnitude,magnitude_error
60003,14.3,0.03
60001,14.1,0.01
60002,14.2,0.02
`,
		),
	)
	if err != nil {
		t.Fatalf(
			"part.Write() error = %v",
			err,
		)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf(
			"multipart writer Close() error = %v",
			err,
		)
	}

	request :=
		httptest.NewRequest(
			http.MethodPost,
			"/api/classify",
			&body,
		)

	request.Header.Set(
		"Content-Type",
		writer.FormDataContentType(),
	)

	response :=
		httptest.NewRecorder()

	server.routes().
		ServeHTTP(
			response,
			request,
		)

	if response.Code !=
		http.StatusOK {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}

	if classifier.calls != 1 {
		t.Fatalf(
			"classifier calls = %d, want 1",
			classifier.calls,
		)
	}

	if len(
		classifier.input.TimeMJD,
	) != 3 {
		t.Fatalf(
			"classifier input epochs = %d, want 3",
			len(classifier.input.TimeMJD),
		)
	}

	// 确认真正送到 Classifier 的输入已经规范化排序。
	if classifier.input.TimeMJD[0] != 60001 ||
		classifier.input.TimeMJD[1] != 60002 ||
		classifier.input.TimeMJD[2] != 60003 {
		t.Fatalf(
			"classifier TimeMJD = %#v",
			classifier.input.TimeMJD,
		)
	}

	var got classifyResponse

	if err := json.Unmarshal(
		response.Body.Bytes(),
		&got,
	); err != nil {
		t.Fatalf(
			"decode response: %v",
			err,
		)
	}

	if got.RequestID == "" {
		t.Fatal(
			"RequestID is empty",
		)
	}

	if got.FileName != "curve.csv" {
		t.Fatalf(
			"FileName = %q, want curve.csv",
			got.FileName,
		)
	}

	if got.EpochCount != 3 {
		t.Fatalf(
			"EpochCount = %d, want 3",
			got.EpochCount,
		)
	}

	if got.CoarseMode !=
		"COMPUTE_CURRENT" {
		t.Fatalf(
			"CoarseMode = %q, want COMPUTE_CURRENT",
			got.CoarseMode,
		)
	}

	if got.ModelBundleVersion !=
		"test-bundle" {
		t.Fatalf(
			"ModelBundleVersion = %q",
			got.ModelBundleVersion,
		)
	}

	if got.PredictedCoarseClass !=
		"RR_LYRAE" {
		t.Fatalf(
			"PredictedCoarseClass = %q, want RR_LYRAE",
			got.PredictedCoarseClass,
		)
	}

	if len(
		got.CoarseProbabilities,
	) != 7 {
		t.Fatalf(
			"coarse probability count = %d",
			len(
				got.CoarseProbabilities,
			),
		)
	}

	if len(
		got.FineConditionalProbabilities,
	) != 10 {
		t.Fatalf(
			"fine probability count = %d",
			len(
				got.FineConditionalProbabilities,
			),
		)
	}

	if len(
		got.LeafProbabilities,
	) != 12 {
		t.Fatalf(
			"leaf probability count = %d",
			len(
				got.LeafProbabilities,
			),
		)
	}
}

func TestScienceServerRejectsInvalidUpload(
	t *testing.T,
) {
	classifier :=
		&stubScienceClassifier{}

	server, err :=
		newScienceServer(
			classifier,
			application.ServingBundleMetadata{
				ModelBundleVersion: "test-bundle",
			},
		)
	if err != nil {
		t.Fatalf(
			"newScienceServer() error = %v",
			err,
		)
	}

	var body bytes.Buffer

	writer :=
		multipart.NewWriter(
			&body,
		)

	part, err :=
		writer.CreateFormFile(
			"file",
			"bad.csv",
		)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = part.Write(
		[]byte(
			`time,magnitude,magnitude_error
60001,14.1,0.01
60001,14.2,0.02
60003,14.3,0.03
`,
		),
	)

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request :=
		httptest.NewRequest(
			http.MethodPost,
			"/api/classify",
			&body,
		)

	request.Header.Set(
		"Content-Type",
		writer.FormDataContentType(),
	)

	response :=
		httptest.NewRecorder()

	server.routes().
		ServeHTTP(
			response,
			request,
		)

	if response.Code !=
		http.StatusBadRequest {
		t.Fatalf(
			"status = %d, want 400; body = %s",
			response.Code,
			response.Body.String(),
		)
	}

	if classifier.calls != 0 {
		t.Fatalf(
			"classifier calls = %d, want 0",
			classifier.calls,
		)
	}
}
