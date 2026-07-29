package application

import (
	"context"
	"errors"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

var (
	// ErrLightCurveRevisionNotFound 表示权威数据源确认指定 object/revision 不存在。
	// 该错误属于永久错误。
	ErrLightCurveRevisionNotFound = errors.New("light curve revision not found")

	// ErrLightCurveRevisionNotReady 表示 revision 已登记，但尚未准备好读取。
	// 该错误属于可重试错误。
	ErrLightCurveRevisionNotReady = errors.New("light curve revision not ready")

	// ErrLightCurveRevisionInconsistent 表示权威数据源确认 revision
	// 内部数据不满足其自身契约。该错误属于永久错误。
	ErrLightCurveRevisionInconsistent = errors.New("light curve revision is inconsistent")

	// ErrLightCurveSourceUnavailable 表示当前无法从上游数据源确认读取结果。
	// 限流、临时服务故障和网络故障可以包装该错误。
	ErrLightCurveSourceUnavailable = errors.New("light curve source unavailable")
)

// LightCurveRepository 是分类应用读取固定光变曲线 revision 的 Port。
//
// 实现必须使用 objectID 和 revision 的精确组合进行查询，
// 禁止回退到 latest revision。
//
// 返回结果从调用方角度视为只读快照。实现不得在返回后继续修改
// LightCurveRevision 或其 Epochs；调用方需要排序时必须先复制。
type LightCurveRepository interface {
	GetRevision(ctx context.Context, objectID string, revision int64) (domain.LightCurveRevision, error)
}
