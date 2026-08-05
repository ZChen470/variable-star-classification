package modelbundle

import (
	"context"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

const servingManifestSchemaVersion = "model-bundle-manifest-v2"

var (
	ErrNilContext = errors.New("context must not be nil")

	ErrInvalidServingBundleManifest = errors.New(
		"invalid serving bundle manifest",
	)
)

var numericModelVersionPattern = regexp.MustCompile(`^[1-9][0-9]*$`)

// FileServingBundleResolver 从一个不可变 manifest 文件加载精确 Serving Bundle。
// manifest 在构造时读取一次，Resolve 不自动重载，也不选择 latest
type FileServingBundleResolver struct {
	metadata application.ServingBundleMetadata
}

var _ application.ServingBundleResolver = (*FileServingBundleResolver)(nil)

// NewFileServingBundleResolver 加载并严格验证 Go Adapter 依赖的 v2 契约。
// DRAFT/PENDING 状态允许加载：真实 Triton 运行验收由后续启动门禁负责
func NewFileServingBundleResolver(path string) (*FileServingBundleResolver, error) {
	if err := requireToken("manifest path", path); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidServingBundleManifest, err)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open serving bundle manifest %q: %w", path, err)
	}
	defer file.Close()

	metadata, err := decodeServingBundleManifest(file)
	if err != nil {
		return nil, fmt.Errorf("load serving bundle manifest %q: %w", path, err)
	}

	return &FileServingBundleResolver{
		metadata: metadata,
	}, nil
}

// ResolveServingBundle 精确匹配 Command 已绑定的 model_bundle_version。
// 请求值不会被 trim、转换大小写或改写
func (resolver *FileServingBundleResolver) ResolveServingBundle(ctx context.Context, modelBundleVersion string) (application.ServingBundleMetadata, error) {
	if ctx == nil {
		return application.ServingBundleMetadata{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return application.ServingBundleMetadata{}, err
	}
	if resolver == nil {
		return application.ServingBundleMetadata{}, fmt.Errorf(
			"%w: resolver is nil",
			ErrInvalidServingBundleManifest,
		)
	}
	if modelBundleVersion != resolver.metadata.ModelBundleVersion {
		return application.ServingBundleMetadata{}, fmt.Errorf(
			"%w: model_bundle_version=%q",
			application.ErrServingBundleNotFound,
			modelBundleVersion,
		)
	}

	return resolver.metadata, nil
}

func decodeServingBundleManifest(reader io.Reader) (application.ServingBundleMetadata, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var manifest servingManifestV2
	if err := decoder.Decode(&manifest); err != nil {
		return application.ServingBundleMetadata{}, fmt.Errorf(
			"%w: decode YAML: %v",
			ErrInvalidServingBundleManifest,
			err,
		)
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return application.ServingBundleMetadata{}, fmt.Errorf(
				"%w: multiple YAML documents are not allowed",
				ErrInvalidServingBundleManifest,
			)
		}
		return application.ServingBundleMetadata{}, fmt.Errorf(
			"%w: decode trailing YAML: %v",
			ErrInvalidServingBundleManifest,
			err,
		)
	}

	if err := validateServingManifest(manifest); err != nil {
		return application.ServingBundleMetadata{}, fmt.Errorf(
			"%w: %v",
			ErrInvalidServingBundleManifest,
			err,
		)
	}

	return mapServingMetadata(manifest), nil
}

func validateServingManifest(manifest servingManifestV2) error {
	if manifest.SchemaVersion != servingManifestSchemaVersion {
		return fmt.Errorf(
			"schema_version=%q, want %q",
			manifest.SchemaVersion,
			servingManifestSchemaVersion,
		)
	}
	if manifest.ManifestStatus != "DRAFT" && manifest.ManifestStatus != "ACTIVE" {
		return fmt.Errorf(
			"manifest_status=%q, want DRAFT or ACTIVE",
			manifest.ManifestStatus,
		)
	}
	if manifest.BundleID != manifest.ModelBundleVersion {
		return fmt.Errorf(
			"bundle_id=%q does not match model_bundle_version=%q",
			manifest.BundleID,
			manifest.ModelBundleVersion,
		)
	}

	if err := validateEntrypoint(manifest.TritonEntrypoint); err != nil {
		return err
	}
	return nil
}

func validateEntrypoint(entrypoint tritonEntrypointManifest) error {
	if entrypoint.ModelName != "variable_star_classifier" {
		return fmt.Errorf(
			"triton_entrypoint.model_name=%q, want variable_star_classifier",
			entrypoint.ModelName,
		)
	}
	if entrypoint.Backend != "python" {
		return fmt.Errorf(
			"triton_entrypoint.backend=%q, want python",
			entrypoint.Backend,
		)
	}
	return nil
}

func validateTensorList(
	kind string,
	got []servingTensorManifest,
	want []servingTensorManifest,
) error {
	if len(got) != len(want) {
		return fmt.Errorf(
			"triton_entrypoint.%s count=%d, want %d",
			kind,
			len(got),
			len(want),
		)
	}
	for index := range want {
		if !reflect.DeepEqual(got[index], want[index]) {
			return fmt.Errorf(
				"triton_entrypoint.%s[%d]=%+v, want %+v",
				kind,
				index,
				got[index],
				want[index],
			)
		}
	}
	return nil
}

func mapServingMetadata(
	manifest servingManifestV2,
) application.ServingBundleMetadata {
	metadata := application.ServingBundleMetadata{
		ModelBundleVersion:     manifest.ModelBundleVersion,
		ServingContractVersion: manifest.ServingContractVersion,
		Entrypoint: application.ServingEntrypointMetadata{
			ModelName:    manifest.TritonEntrypoint.ModelName,
			ModelVersion: manifest.TritonEntrypoint.ModelVersion,
			Backend:      manifest.TritonEntrypoint.Backend,
			Protocol: application.ServingProtocol(
				manifest.TritonEntrypoint.Protocol,
			),
			BinaryTensorData: manifest.TritonEntrypoint.BinaryTensorData,
			MaxBatchSize:     manifest.TritonEntrypoint.MaxBatchSize,
			Inputs: mapTensorContracts(
				manifest.TritonEntrypoint.Inputs,
			),
			Outputs: mapTensorContracts(
				manifest.TritonEntrypoint.Outputs,
			),
		},
	}

	copy(
		metadata.CoarseProbabilityOrder[:],
		manifest.XGBoost.CoarseClassOrder.ExpectedOrder,
	)
	copy(
		metadata.ConditionalFineProbabilityOrder[:],
		manifest.FineConditionalProbabilityOrder,
	)
	copy(
		metadata.LeafProbabilityOrder[:],
		manifest.LeafProbabilityOrder,
	)

	return metadata
}

func mapTensorContracts(
	tensors []servingTensorManifest,
) []application.ServingTensorContract {
	mapped := make([]application.ServingTensorContract, len(tensors))
	for index, tensor := range tensors {
		mapped[index] = application.ServingTensorContract{
			Name:     tensor.Name,
			DataType: application.TensorDataType(tensor.DataType),
			Dims:     append([]int64(nil), tensor.Dims...),
			Required: tensor.Required,
		}
	}
	return mapped
}

func requireToken(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s=%q has surrounding whitespace", name, value)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s contains NUL", name)
	}
	return nil
}

// 以下结构覆盖 v2 顶层和 Go Adapter 依赖的内部字段。
// 其他科学实现细节使用 yaml.Node 保存，不由 Go Loader 重复解释。
type servingManifestV2 struct {
	SchemaVersion          string `yaml:"schema_version"`
	ManifestStatus         string `yaml:"manifest_status"`
	BundleID               string `yaml:"bundle_id"`
	ModelBundleVersion     string `yaml:"model_bundle_version"`
	ServingContractVersion string `yaml:"serving_contract_version"`

	TritonEntrypoint tritonEntrypointManifest `yaml:"triton_entrypoint"`

	CoarseModes   yaml.Node       `yaml:"coarse_modes"`
	Preprocessing yaml.Node       `yaml:"preprocessing"`
	XGBoost       xgboostManifest `yaml:"xgboost"`
	Transformer   yaml.Node       `yaml:"transformer"`

	FineConditionalProbabilityOrder []string `yaml:"fine_conditional_probability_order"`
	LeafProbabilityOrder            []string `yaml:"leaf_probability_order"`

	Fusion              yaml.Node `yaml:"fusion"`
	RuntimeRequirements yaml.Node `yaml:"runtime_requirements"`
	ContractArtifacts   yaml.Node `yaml:"contract_artifacts"`
	Approval            yaml.Node `yaml:"approval"`
}

type tritonEntrypointManifest struct {
	ModelName                 string                  `yaml:"model_name"`
	ModelVersion              string                  `yaml:"model_version"`
	Backend                   string                  `yaml:"backend"`
	MaxBatchSize              int                     `yaml:"max_batch_size"`
	Protocol                  string                  `yaml:"protocol"`
	BinaryTensorData          bool                    `yaml:"binary_tensor_data"`
	RuntimeVerificationStatus string                  `yaml:"runtime_verification_status"`
	ConfigPath                string                  `yaml:"config_path"`
	ImplementationPath        string                  `yaml:"implementation_path"`
	Inputs                    []servingTensorManifest `yaml:"inputs"`
	Outputs                   []servingTensorManifest `yaml:"outputs"`
}

type servingTensorManifest struct {
	Name      string  `yaml:"name"`
	DataType  string  `yaml:"datatype"`
	Dims      []int64 `yaml:"dims"`
	Semantics string  `yaml:"semantics,omitempty"`
	Required  bool    `yaml:"required,omitempty"`
}

type xgboostManifest struct {
	FeatureExtraction yaml.Node `yaml:"feature_extraction"`
	Scaler            yaml.Node `yaml:"scaler"`
	Classifier        yaml.Node `yaml:"classifier"`

	CoarseClassOrder coarseClassOrderManifest `yaml:"coarse_class_order"`

	CoarseCompatibilityID string `yaml:"coarse_compatibility_id"`
}

type coarseClassOrderManifest struct {
	VerificationStatus string   `yaml:"verification_status"`
	ExpectedOrder      []string `yaml:"expected_order"`
}
