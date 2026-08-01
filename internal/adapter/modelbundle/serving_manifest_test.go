package modelbundle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

const testServingBundleVersion = "variable-classifier-2026-07-003"

func TestFileServingBundleResolverLoadsRepositoryManifest(t *testing.T) {
	resolver, err := NewFileServingBundleResolver(repositoryManifestPath(t))
	if err != nil {
		t.Fatalf("NewFileServingBundleResolver() error = %v", err)
	}

	metadata, err := resolver.ResolveServingBundle(
		context.Background(),
		testServingBundleVersion,
	)
	if err != nil {
		t.Fatalf("ResolveServingBundle() error = %v", err)
	}

	if metadata.ModelBundleVersion != testServingBundleVersion {
		t.Fatalf(
			"ModelBundleVersion = %q, want %q",
			metadata.ModelBundleVersion,
			testServingBundleVersion,
		)
	}

	if metadata.Entrypoint.ModelName != "variable_star_classifier" {
		t.Fatalf(
			"ModelName = %q, want variable_star_classifier",
			metadata.Entrypoint.ModelName,
		)
	}

	if metadata.Entrypoint.Backend != "python" {
		t.Fatalf(
			"Backend = %q, want python",
			metadata.Entrypoint.Backend,
		)
	}

	if metadata.Entrypoint.ModelVersion != "1" {
		t.Fatalf(
			"ModelVersion = %q, want 1",
			metadata.Entrypoint.ModelVersion,
		)
	}

	if len(metadata.Entrypoint.Inputs) != 5 {
		t.Fatalf(
			"input count = %d, want 5",
			len(metadata.Entrypoint.Inputs),
		)
	}

	if len(metadata.Entrypoint.Outputs) != 4 {
		t.Fatalf(
			"output count = %d, want 4",
			len(metadata.Entrypoint.Outputs),
		)
	}

	assertTensorContract(
		t,
		metadata.Entrypoint.Inputs[0],
		"TIME_MJD",
		application.TensorDataTypeFP64,
		[]int64{-1},
		true,
	)

	assertTensorContract(
		t,
		metadata.Entrypoint.Inputs[4],
		"REUSED_COARSE_PROBS",
		application.TensorDataTypeFP32,
		[]int64{7},
		true,
	)

	assertTensorContract(
		t,
		metadata.Entrypoint.Outputs[0],
		"COARSE_PROBS",
		application.TensorDataTypeFP32,
		[]int64{7},
		false,
	)

	assertTensorContract(
		t,
		metadata.Entrypoint.Outputs[3],
		"XGBOOST_EXECUTED",
		application.TensorDataTypeBOOL,
		[]int64{1},
		false,
	)

	if len(metadata.CoarseProbabilityOrder) != 7 {
		t.Fatalf(
			"coarse probability order length = %d, want 7",
			len(metadata.CoarseProbabilityOrder),
		)
	}

	if metadata.CoarseProbabilityOrder[0] != "ROTATING" ||
		metadata.CoarseProbabilityOrder[6] != "SUPERNOVA" {
		t.Fatalf(
			"CoarseProbabilityOrder = %v",
			metadata.CoarseProbabilityOrder,
		)
	}

	if len(metadata.ConditionalFineProbabilityOrder) != 10 {
		t.Fatalf(
			"fine probability order length = %d, want 10",
			len(metadata.ConditionalFineProbabilityOrder),
		)
	}

	if metadata.ConditionalFineProbabilityOrder[0] != "EW" ||
		metadata.ConditionalFineProbabilityOrder[9] != "CEP" {
		t.Fatalf(
			"ConditionalFineProbabilityOrder = %v",
			metadata.ConditionalFineProbabilityOrder,
		)
	}

	if len(metadata.LeafProbabilityOrder) != 12 {
		t.Fatalf(
			"leaf probability order length = %d, want 12",
			len(metadata.LeafProbabilityOrder),
		)
	}

	if metadata.LeafProbabilityOrder[0] != "EW" ||
		metadata.LeafProbabilityOrder[9] != "CEP" ||
		metadata.LeafProbabilityOrder[10] != "CATACLYSMIC" ||
		metadata.LeafProbabilityOrder[11] != "SUPERNOVA" {
		t.Fatalf(
			"LeafProbabilityOrder = %v",
			metadata.LeafProbabilityOrder,
		)
	}
}

func TestFileServingBundleResolverRequiresExactVersion(t *testing.T) {
	resolver, err := NewFileServingBundleResolver(repositoryManifestPath(t))
	if err != nil {
		t.Fatalf("NewFileServingBundleResolver() error = %v", err)
	}

	tests := []string{
		"",
		"latest",
		" " + testServingBundleVersion,
		testServingBundleVersion + " ",
		strings.ToUpper(testServingBundleVersion),
		"variable-classifier-2026-07-004",
	}

	for _, version := range tests {
		t.Run(version, func(t *testing.T) {
			_, resolveErr := resolver.ResolveServingBundle(
				context.Background(),
				version,
			)

			if !errors.Is(
				resolveErr,
				application.ErrServingBundleNotFound,
			) {
				t.Fatalf(
					"ResolveServingBundle(%q) error = %v, want ErrServingBundleNotFound",
					version,
					resolveErr,
				)
			}
		})
	}
}

func TestFileServingBundleResolverContextHandling(t *testing.T) {
	resolver, err := NewFileServingBundleResolver(repositoryManifestPath(t))
	if err != nil {
		t.Fatalf("NewFileServingBundleResolver() error = %v", err)
	}

	_, err = resolver.ResolveServingBundle(
		nil,
		testServingBundleVersion,
	)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf(
			"nil context error = %v, want ErrNilContext",
			err,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = resolver.ResolveServingBundle(
		ctx,
		testServingBundleVersion,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"cancelled context error = %v, want context.Canceled",
			err,
		)
	}
}

func TestNilFileServingBundleResolverIsRejected(t *testing.T) {
	var resolver *FileServingBundleResolver

	_, err := resolver.ResolveServingBundle(
		context.Background(),
		testServingBundleVersion,
	)
	if !errors.Is(err, ErrInvalidServingBundleManifest) {
		t.Fatalf(
			"nil resolver error = %v, want ErrInvalidServingBundleManifest",
			err,
		)
	}
}

func TestNewFileServingBundleResolverRejectsInvalidPath(t *testing.T) {
	tests := []string{
		"",
		" manifest.yaml",
		"manifest.yaml ",
		"manifest\x00.yaml",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			_, err := NewFileServingBundleResolver(path)
			if !errors.Is(err, ErrInvalidServingBundleManifest) {
				t.Fatalf(
					"NewFileServingBundleResolver(%q) error = %v, want ErrInvalidServingBundleManifest",
					path,
					err,
				)
			}
		})
	}
}

func TestNewFileServingBundleResolverRejectsMissingFile(t *testing.T) {
	path := filepath.Join(
		t.TempDir(),
		"missing-manifest.yaml",
	)

	_, err := NewFileServingBundleResolver(path)
	if err == nil {
		t.Fatal("NewFileServingBundleResolver() error = nil")
	}
}

func TestNewFileServingBundleResolverRejectsInvalidManifest(t *testing.T) {
	valid := readRepositoryManifest(t)

	tests := []struct {
		name   string
		mutate func(*testing.T, string) string
	}{
		{
			name: "wrong schema version",
			mutate: func(t *testing.T, content string) string {
				return replaceFirstYAMLScalar(
					t,
					content,
					"schema_version",
					"model-bundle-manifest-v3",
				)
			},
		},
		{
			name: "unsupported manifest status",
			mutate: func(t *testing.T, content string) string {
				return replaceFirstYAMLScalar(
					t,
					content,
					"manifest_status",
					"RETIRED",
				)
			},
		},
		{
			name: "bundle identity mismatch",
			mutate: func(t *testing.T, content string) string {
				return replaceFirstYAMLScalar(
					t,
					content,
					"bundle_id",
					"different-bundle",
				)
			},
		},
		{
			name: "wrong Triton model name",
			mutate: func(t *testing.T, content string) string {
				return replaceFirstYAMLScalar(
					t,
					content,
					"model_name",
					"another_classifier",
				)
			},
		},
		{
			name: "wrong Triton backend",
			mutate: func(t *testing.T, content string) string {
				return replaceFirstYAMLScalar(
					t,
					content,
					"backend",
					"onnxruntime",
				)
			},
		},
		{
			name: "unknown top-level field",
			mutate: func(_ *testing.T, content string) string {
				return content + "\nunknown_top_level: true\n"
			},
		},
		{
			name: "multiple YAML documents",
			mutate: func(_ *testing.T, content string) string {
				return content +
					"\n---\nschema_version: model-bundle-manifest-v2\n"
			},
		},
		{
			name: "malformed YAML",
			mutate: func(_ *testing.T, content string) string {
				return content + "\ninvalid: [\n"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTempManifest(
				t,
				test.mutate(t, valid),
			)

			_, err := NewFileServingBundleResolver(path)
			if !errors.Is(
				err,
				ErrInvalidServingBundleManifest,
			) {
				t.Fatalf(
					"NewFileServingBundleResolver() error = %v, want ErrInvalidServingBundleManifest",
					err,
				)
			}
		})
	}
}

func assertTensorContract(
	t *testing.T,
	actual application.ServingTensorContract,
	wantName string,
	wantDataType application.TensorDataType,
	wantDims []int64,
	wantRequired bool,
) {
	t.Helper()

	if actual.Name != wantName {
		t.Fatalf(
			"tensor name = %q, want %q",
			actual.Name,
			wantName,
		)
	}

	if actual.DataType != wantDataType {
		t.Fatalf(
			"tensor %s datatype = %q, want %q",
			wantName,
			actual.DataType,
			wantDataType,
		)
	}

	if !equalInt64Slices(actual.Dims, wantDims) {
		t.Fatalf(
			"tensor %s dims = %v, want %v",
			wantName,
			actual.Dims,
			wantDims,
		)
	}

	if actual.Required != wantRequired {
		t.Fatalf(
			"tensor %s required = %t, want %t",
			wantName,
			actual.Required,
			wantRequired,
		)
	}
}

func equalInt64Slices(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}

	return true
}

func repositoryManifestPath(t *testing.T) string {
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

func readRepositoryManifest(t *testing.T) string {
	t.Helper()

	content, err := os.ReadFile(repositoryManifestPath(t))
	if err != nil {
		t.Fatalf(
			"os.ReadFile() error = %v",
			err,
		)
	}

	return string(content)
}

func writeTempManifest(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(
		t.TempDir(),
		"manifest.yaml",
	)

	if err := os.WriteFile(
		path,
		[]byte(content),
		0o600,
	); err != nil {
		t.Fatalf(
			"os.WriteFile() error = %v",
			err,
		)
	}

	return path
}

func replaceFirstYAMLScalar(
	t *testing.T,
	content string,
	key string,
	value string,
) string {
	t.Helper()

	lines := strings.Split(content, "\n")
	prefix := key + ":"

	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, prefix) {
			continue
		}

		indentLength := len(line) - len(strings.TrimLeft(line, " \t"))
		indent := line[:indentLength]

		lines[index] = indent + prefix + " " + value
		return strings.Join(lines, "\n")
	}

	t.Fatalf("YAML key %q was not found", key)
	return ""
}
