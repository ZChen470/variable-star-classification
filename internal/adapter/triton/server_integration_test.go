package triton

import (
	"context"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/adapter/modelbundle"
	"github.com/ZChen470/variable-star-classification/internal/application"
)

const serverIntegrationBundleVersion = "variable-classifier-2026-07-003"

func TestTritonServerIntegration(t *testing.T) {
	if os.Getenv("VSC_TRITON_INTEGRATION") != "1" {
		t.Skip("set VSC_TRITON_INTEGRATION=1 to run against a real Triton server")
	}

	baseURL := os.Getenv("VSC_TRITON_BASE_URL")
	if baseURL == "" {
		t.Fatal("VSC_TRITON_BASE_URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	resolver, err := modelbundle.NewFileServingBundleResolver(
		serverIntegrationManifestPath(t),
	)
	if err != nil {
		t.Fatalf("NewFileServingBundleResolver() error = %v", err)
	}

	bundle, err := resolver.ResolveServingBundle(
		ctx,
		serverIntegrationBundleVersion,
	)
	if err != nil {
		t.Fatalf("ResolveServingBundle() error = %v", err)
	}

	client, err := NewClient(
		baseURL,
		&http.Client{Timeout: 3 * time.Minute},
		4<<20,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	if err := gate.Verify(ctx, bundle.Entrypoint); err != nil {
		t.Fatalf("ModelContractGate.Verify() error = %v", err)
	}

	classifier, err := NewVariableStarClassifier(client, bundle.Entrypoint)
	if err != nil {
		t.Fatalf("NewVariableStarClassifier() error = %v", err)
	}

	bootstrapInput := serverIntegrationInput(
		21,
		application.CoarseModeComputeBootstrap,
		nil,
	)

	bootstrap, err := classifier.Classify(ctx, bootstrapInput)
	if err != nil {
		t.Fatalf("COMPUTE_BOOTSTRAP Classify() error = %v", err)
	}

	if !bootstrap.XGBoostExecuted {
		t.Fatal("COMPUTE_BOOTSTRAP returned XGBoostExecuted=false")
	}

	assertServerIntegrationOutput(t, "COMPUTE_BOOTSTRAP", bootstrap)

	reusedCoarse := bootstrap.CoarseProbabilities
	reuseInput := serverIntegrationInput(
		21,
		application.CoarseModeReusePrevious,
		&reusedCoarse,
	)

	reuse, err := classifier.Classify(ctx, reuseInput)
	if err != nil {
		t.Fatalf("REUSE_PREVIOUS Classify() error = %v", err)
	}

	if reuse.XGBoostExecuted {
		t.Fatal("REUSE_PREVIOUS returned XGBoostExecuted=true")
	}

	for index := range reusedCoarse {
		if reuse.CoarseProbabilities[index] != reusedCoarse[index] {
			t.Fatalf(
				"REUSE_PREVIOUS coarse[%d]=%v, want exact reused value %v",
				index,
				reuse.CoarseProbabilities[index],
				reusedCoarse[index],
			)
		}
	}

	assertServerIntegrationOutput(t, "REUSE_PREVIOUS", reuse)

	t.Logf(
		"real Triton inference passed: model=%s version=%s bootstrap_coarse=%v reuse_leaf=%v",
		bundle.Entrypoint.ModelName,
		bundle.Entrypoint.ModelVersion,
		bootstrap.CoarseProbabilities,
		reuse.LeafProbabilities,
	)
}

func serverIntegrationInput(
	epochCount int,
	mode application.CoarseMode,
	reused *[application.CoarseClassCount]float32,
) application.ClassificationInput {
	timeMJD := make([]float64, epochCount)
	magnitude := make([]float32, epochCount)
	magnitudeError := make([]float32, epochCount)

	for index := 0; index < epochCount; index++ {
		timeMJD[index] = 60000.25 + float64(index)*0.75
		magnitude[index] = float32(
			15.0 + 0.3*math.Sin(float64(index)*0.35),
		)
		magnitudeError[index] = 0.05 + float32(index%3)*0.001
	}

	return application.ClassificationInput{
		TimeMJD:                   timeMJD,
		Magnitude:                 magnitude,
		MagnitudeError:            magnitudeError,
		CoarseMode:                mode,
		ReusedCoarseProbabilities: reused,
	}
}

func assertServerIntegrationOutput(
	t *testing.T,
	name string,
	output application.ClassificationOutput,
) {
	t.Helper()

	assertServerProbabilityVector(
		t,
		name+" COARSE_PROBS",
		output.CoarseProbabilities[:],
		1,
	)

	for start := 0; start < application.ConditionalFineClassCount; start += 2 {
		assertServerProbabilityVector(
			t,
			name+" FINE_CONDITIONAL_PROBS",
			output.ConditionalFineProbabilities[start:start+2],
			1,
		)
	}

	assertServerProbabilityVector(
		t,
		name+" LEAF_PROBS",
		output.LeafProbabilities[:],
		1,
	)
}

func assertServerProbabilityVector(
	t *testing.T,
	name string,
	values []float32,
	expectedSum float64,
) {
	t.Helper()

	var sum float64
	for index, value := range values {
		asFloat64 := float64(value)

		if math.IsNaN(asFloat64) ||
			math.IsInf(asFloat64, 0) ||
			value < 0 ||
			value > 1 {
			t.Fatalf("%s[%d]=%v is not a probability", name, index, value)
		}

		sum += asFloat64
	}

	if math.Abs(sum-expectedSum) > 1e-5 {
		t.Fatalf(
			"%s sum=%0.10f, want %0.10f within 1e-5",
			name,
			sum,
			expectedSum,
		)
	}
}

func serverIntegrationManifestPath(t *testing.T) string {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}

	return filepath.Clean(
		filepath.Join(
			filepath.Dir(currentFile),
			"..",
			"..",
			"..",
			"models",
			"bundles",
			"model-bundle-manifest-v2.yaml",
		),
	)
}
