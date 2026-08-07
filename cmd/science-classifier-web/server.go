package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/google/uuid"
)

const inferenceTimeout = 30 * time.Second

//go:embed index.html
var indexHTML []byte

type scienceServer struct {
	classifier application.
			VariableStarClassifier

	servingBundle application.
			ServingBundleMetadata
}

type probabilityItem struct {
	Class string `json:"class"`

	Probability float32 `json:"probability"`
}

type classifyResponse struct {
	RequestID string `json:"request_id"`

	FileName string `json:"file_name"`

	EpochCount int `json:"epoch_count"`

	CoarseMode string `json:"coarse_mode"`

	ModelBundleVersion string `json:"model_bundle_version"`

	ModelName string `json:"model_name"`

	ModelVersion string `json:"model_version"`

	XGBoostExecuted bool `json:"xgboost_executed"`

	PredictedCoarseClass string `json:"predicted_coarse_class"`

	MaximumCoarseProbability float32 `json:"maximum_coarse_probability"`

	PredictedLeafClass string `json:"predicted_leaf_class"`

	MaximumLeafProbability float32 `json:"maximum_leaf_probability"`

	CoarseProbabilities []probabilityItem `json:"coarse_probabilities"`

	FineConditionalProbabilities []probabilityItem `json:"fine_conditional_probabilities"`

	LeafProbabilities []probabilityItem `json:"leaf_probabilities"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func newScienceServer(
	classifier application.
		VariableStarClassifier,

	servingBundle application.
		ServingBundleMetadata,
) (*scienceServer, error) {
	if classifier == nil {
		return nil,
			errors.New(
				"classifier must not be nil",
			)
	}

	if servingBundle.
		ModelBundleVersion == "" {
		return nil,
			errors.New(
				"serving bundle model version must not be empty",
			)
	}

	return &scienceServer{
		classifier: classifier,

		servingBundle: servingBundle,
	}, nil
}

func (
	server *scienceServer,
) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"GET /",
		server.handleIndex,
	)

	mux.HandleFunc(
		"GET /healthz",
		server.handleHealth,
	)

	mux.HandleFunc(
		"POST /api/classify",
		server.handleClassify,
	)

	return mux
}

func (
	server *scienceServer,
) handleIndex(
	writer http.ResponseWriter,
	_ *http.Request,
) {
	writer.Header().Set(
		"Content-Type",
		"text/html; charset=utf-8",
	)

	writer.Header().Set(
		"Cache-Control",
		"no-store",
	)

	_, _ = writer.Write(
		indexHTML,
	)
}

func (
	server *scienceServer,
) handleHealth(
	writer http.ResponseWriter,
	_ *http.Request,
) {
	writer.Header().Set(
		"Content-Type",
		"text/plain; charset=utf-8",
	)

	writer.WriteHeader(
		http.StatusOK,
	)

	_, _ = writer.Write(
		[]byte("ok\n"),
	)
}

func (
	server *scienceServer,
) handleClassify(
	writer http.ResponseWriter,
	request *http.Request,
) {
	request.Body =
		http.MaxBytesReader(
			writer,
			request.Body,
			maxUploadBytes+
				256*1024,
		)

	if err :=
		request.ParseMultipartForm(
			maxUploadBytes,
		); err != nil {
		writeJSONError(
			writer,
			http.StatusBadRequest,
			fmt.Sprintf(
				"invalid multipart upload: %v",
				err,
			),
		)

		return
	}

	file, header, err :=
		request.FormFile("file")
	if err != nil {
		writeJSONError(
			writer,
			http.StatusBadRequest,
			"missing multipart file field named 'file'",
		)

		return
	}

	defer file.Close()

	epochs, err :=
		parseUploadedLightCurve(
			io.LimitReader(
				file,
				maxUploadBytes+1,
			),
		)
	if err != nil {
		writeJSONError(
			writer,
			http.StatusBadRequest,
			err.Error(),
		)

		return
	}

	input, mode, err :=
		buildScienceClassificationInput(
			epochs,
		)
	if err != nil {
		writeJSONError(
			writer,
			http.StatusBadRequest,
			err.Error(),
		)

		return
	}

	requestID :=
		uuid.NewString()

	ctx, cancel :=
		context.WithTimeout(
			request.Context(),
			inferenceTimeout,
		)
	defer cancel()

	ctx =
		application.
			WithClassificationRequestID(
				ctx,
				requestID,
			)

	output, err :=
		server.classifier.Classify(
			ctx,
			input,
		)
	if err != nil {
		status :=
			http.StatusBadGateway

		if errors.Is(
			err,
			context.DeadlineExceeded,
		) {
			status =
				http.StatusGatewayTimeout
		} else if errors.Is(
			err,
			context.Canceled,
		) {
			status =
				http.StatusRequestTimeout
		}

		writeJSONError(
			writer,
			status,
			fmt.Sprintf(
				"classification failed: %v",
				err,
			),
		)

		return
	}

	response :=
		server.buildResponse(
			requestID,
			header.Filename,
			len(input.TimeMJD),
			mode,
			output,
		)

	writer.Header().Set(
		"Content-Type",
		"application/json; charset=utf-8",
	)

	writer.Header().Set(
		"Cache-Control",
		"no-store",
	)

	writer.WriteHeader(
		http.StatusOK,
	)

	_ = json.NewEncoder(
		writer,
	).Encode(response)
}

func (
	server *scienceServer,
) buildResponse(
	requestID string,
	fileName string,
	epochCount int,
	mode application.CoarseMode,
	output application.
		ClassificationOutput,
) classifyResponse {
	coarse :=
		makeProbabilityItems(
			server.servingBundle.
				CoarseProbabilityOrder[:],

			output.
				CoarseProbabilities[:],
		)

	fine :=
		makeProbabilityItems(
			server.servingBundle.
				ConditionalFineProbabilityOrder[:],

			output.
				ConditionalFineProbabilities[:],
		)

	leaf :=
		makeProbabilityItems(
			server.servingBundle.
				LeafProbabilityOrder[:],

			output.
				LeafProbabilities[:],
		)

	predictedCoarse,
		maxCoarse :=
		maxProbability(coarse)

	predictedLeaf,
		maxLeaf :=
		maxProbability(leaf)

	return classifyResponse{
		RequestID: requestID,

		FileName: fileName,

		EpochCount: epochCount,

		CoarseMode: coarseModeName(mode),

		ModelBundleVersion: server.servingBundle.
			ModelBundleVersion,

		ModelName: server.servingBundle.
			Entrypoint.
			ModelName,

		ModelVersion: server.servingBundle.
			Entrypoint.
			ModelVersion,

		XGBoostExecuted: output.XGBoostExecuted,

		PredictedCoarseClass: predictedCoarse,

		MaximumCoarseProbability: maxCoarse,

		PredictedLeafClass: predictedLeaf,

		MaximumLeafProbability: maxLeaf,

		CoarseProbabilities: coarse,

		FineConditionalProbabilities: fine,

		LeafProbabilities: leaf,
	}
}

func makeProbabilityItems(
	labels []string,
	probabilities []float32,
) []probabilityItem {
	items := make(
		[]probabilityItem,
		0,
		len(probabilities),
	)

	for index, probability := range probabilities {
		label :=
			fmt.Sprintf(
				"INDEX_%d",
				index,
			)

		if index < len(labels) &&
			strings.TrimSpace(
				labels[index],
			) != "" {
			label =
				labels[index]
		}

		items = append(
			items,
			probabilityItem{
				Class: label,

				Probability: probability,
			},
		)
	}

	return items
}

func maxProbability(
	items []probabilityItem,
) (string, float32) {
	if len(items) == 0 {
		return "", 0
	}

	best := items[0]

	for index := 1; index < len(items); index++ {
		if items[index].
			Probability >
			best.Probability {
			best = items[index]
		}
	}

	return best.Class,
		best.Probability
}

func coarseModeName(
	mode application.CoarseMode,
) string {
	switch mode {
	case application.
		CoarseModeComputeCurrent:
		return "COMPUTE_CURRENT"

	case application.
		CoarseModeComputeBootstrap:
		return "COMPUTE_BOOTSTRAP"

	case application.
		CoarseModeReusePrevious:
		return "REUSE_PREVIOUS"

	default:
		return "UNSPECIFIED"
	}
}

func writeJSONError(
	writer http.ResponseWriter,
	status int,
	message string,
) {
	writer.Header().Set(
		"Content-Type",
		"application/json; charset=utf-8",
	)

	writer.Header().Set(
		"Cache-Control",
		"no-store",
	)

	writer.WriteHeader(status)

	_ = json.NewEncoder(
		writer,
	).Encode(
		errorResponse{
			Error: message,
		},
	)
}
