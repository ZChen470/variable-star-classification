package domain

import "time"

const (
	CoarseProbabilityCount          = 7
	ConditionalFineProbabilityCount = 10
	LeafProbabilityCount            = 12
)

// CoarseSourceType 表示一次成功分类所使用的粗分类概率来源。
type CoarseSourceType uint8

const (
	CoarseSourceTypeUnspecified CoarseSourceType = iota
	CoarseSourceComputedCurrent
	CoarseSourceReusedPrevious
	CoarseSourceComputedBootstrap
)

// IsValid 判断粗分类来源是否为已知业务值。
func (sourceType CoarseSourceType) IsValid() bool {
	switch sourceType {
	case CoarseSourceComputedCurrent,
		CoarseSourceReusedPrevious,
		CoarseSourceComputedBootstrap:
		return true
	default:
		return false
	}
}

// CoarseClass 使用 Proto 中冻结的粗分类枚举值。
type CoarseClass int32

const (
	CoarseClassUnspecified     CoarseClass = 0
	CoarseClassPulsating       CoarseClass = 10
	CoarseClassLongPeriod      CoarseClass = 20
	CoarseClassCataclysmic     CoarseClass = 30
	CoarseClassRRLyrae         CoarseClass = 40
	CoarseClassRotating        CoarseClass = 50
	CoarseClassEclipsingBinary CoarseClass = 60
	CoarseClassSupernova       CoarseClass = 70
)

// IsValid 判断粗分类是否可以出现在成功结果中。
func (class CoarseClass) IsValid() bool {
	switch class {
	case CoarseClassPulsating,
		CoarseClassLongPeriod,
		CoarseClassCataclysmic,
		CoarseClassRRLyrae,
		CoarseClassRotating,
		CoarseClassEclipsingBinary,
		CoarseClassSupernova:
		return true
	default:
		return false
	}
}

// LeafClass 使用 Proto 中冻结的叶分类枚举值。
type LeafClass int32

const (
	LeafClassUnspecified LeafClass = 0

	LeafClassCataclysmic LeafClass = 30
	LeafClassSupernova   LeafClass = 70

	LeafClassDSCT LeafClass = 1001
	LeafClassCEP  LeafClass = 1002

	LeafClassSR   LeafClass = 2001
	LeafClassMira LeafClass = 2002

	LeafClassRRAB LeafClass = 4001
	LeafClassRRC  LeafClass = 4002

	LeafClassByDra LeafClass = 5001
	LeafClassRSCvn LeafClass = 5002

	LeafClassEW LeafClass = 6001
	LeafClassEA LeafClass = 6002
)

// IsValid 判断叶分类是否可以出现在成功结果中。
func (class LeafClass) IsValid() bool {
	switch class {
	case LeafClassCataclysmic,
		LeafClassSupernova,
		LeafClassDSCT,
		LeafClassCEP,
		LeafClassSR,
		LeafClassMira,
		LeafClassRRAB,
		LeafClassRRC,
		LeafClassByDra,
		LeafClassRSCvn,
		LeafClassEW,
		LeafClassEA:
		return true
	default:
		return false
	}
}

// ResolvedModelVersions 保存一次分类实际解析出的全部模型与 Schema 版本。
type ResolvedModelVersions struct {
	ModelBundleVersion          string
	TaxonomyVersion             string
	XGBoostModelVersion         string
	TransformerModelVersion     string
	PreprocessingVersion        string
	FeatureSchemaVersion        string
	TensorSchemaVersion         string
	ClassificationPolicyVersion string
}

// ClassificationTiming 保存一次成功分类各阶段的耗时。
type ClassificationTiming struct {
	DataFetchMS            uint64
	PreprocessingMS        uint64
	XGBoostInferenceMS     uint64
	TransformerInferenceMS uint64
	FusionMS               uint64
	TotalMS                uint64
}

// ClassificationRun 是数据库保存的一次成功分类事实。
//
// PersistedAt 由数据库生成；保存新 Run 时允许为零值。
type ClassificationRun struct {
	RunID    RunID
	JobID    JobID
	ObjectID string

	CandidateRevision   int64
	LightCurveRevision  int64
	EffectiveEpochCount uint32
	ExecutionMode       ExecutionMode

	CoarseSourceType               CoarseSourceType
	CoarseSourceRunID              *RunID
	CoarseSourceLightCurveRevision int64
	CoarseSourceEpochCount         uint32
	XGBoostExecuted                bool

	Versions ResolvedModelVersions

	CoarseProbabilities          [CoarseProbabilityCount]float32
	FineConditionalProbabilities [ConditionalFineProbabilityCount]float32
	LeafProbabilities            [LeafProbabilityCount]float32

	PredictedCoarseClass CoarseClass
	PredictedLeafClass   LeafClass

	Timing ClassificationTiming

	CompletedAt time.Time
	PersistedAt time.Time
}

// CurrentClassification 是对象当前分类指针与其引用的完整 Run。
type CurrentClassification struct {
	Run       ClassificationRun
	UpdatedAt time.Time
}
