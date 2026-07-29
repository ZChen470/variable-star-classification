package domain

// LightCurveEpoch 表示上游完成科学质量筛选后
// 向分类系统提供的一个可用于分类的光变曲线 epoch
type LightCurveEpoch struct {
	ObservationTime float64
	Magnitude       float32
	MagnitudeError  float32
}

// LightCurveRevision 表示上游发布的一个不可变光变曲线版本
//
// EligibleEpochCount 和 QualityPolicyVersion 是上游 revision 元数据
// 分类系统可以校验这些元数据，但不解释或重新执行上游科学质量策略
//
// Epochs 从调用方角度视为只读；需要排序或修改时必须先复制。
type LightCurveRevision struct {
	ObjectID string
	Revision int64

	EligibleEpochCount   uint32
	QualityPolicyVersion *string

	Epochs []LightCurveEpoch
}
