package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

var (
	// ErrLightCurveRevisionIdentityMismatch 表示上游返回的 object/revision
	// 与分类应用请求的固定读取键不一致
	ErrLightCurveRevisionIdentityMismatch = errors.New("light curve revision identity mismatch")
)

// LightCurveRevisionReader 负责将按固定 objectID 和 revision 读取光变曲线
// 并核对上有返回结果的身份
type LightCurveRevisionReader struct {
	repository LightCurveRepository
}

// NewLightCurveRevisionReader 创建固定 revision 读取用例
func NewLightCurveRevisionReader(repository LightCurveRepository) (*LightCurveRevisionReader, error) {
	if repository == nil {
		return nil, errors.New("light curve repository must not be nil")
	}

	return &LightCurveRevisionReader{
		repository: repository,
	}, nil
}

// ReadRevision 使用未经改写的 objectID 和 revision 查询 Repository
//
// 本方法只核对返回结果的 object/revision 身份，不执行 epoch 数量、
// 数值、排序或重复检查。返回结果与 Repository 返回的数据不共享
// Epochs 或 QualityPolicyVersion
func (reader *LightCurveRevisionReader) ReadRevision(ctx context.Context, objectID string, revision int64) (domain.LightCurveRevision, error) {
	if ctx == nil {
		return domain.LightCurveRevision{}, errors.New("read light curve revision: nil context")
	}
	if reader == nil {
		return domain.LightCurveRevision{}, errors.New("read light curve revision: nil reader")
	}
	if reader.repository == nil {
		return domain.LightCurveRevision{}, errors.New("read light curve revision: nil repository")
	}
	if err := ctx.Err(); err != nil {
		return domain.LightCurveRevision{}, err
	}

	lightCurve, err := reader.repository.GetRevision(ctx, objectID, revision)
	if err != nil {
		return domain.LightCurveRevision{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.LightCurveRevision{}, err
	}

	if lightCurve.ObjectID != objectID || lightCurve.Revision != revision {
		return domain.LightCurveRevision{}, fmt.Errorf(
			"%w: requested object_id=%q revision=%d, returned object_id=%q revision=%d",
			ErrLightCurveRevisionIdentityMismatch,
			objectID,
			revision,
			lightCurve.ObjectID,
			lightCurve.Revision,
		)
	}

	return lightCurve, nil
}

//func cloneLightCurveRevision(revision domain.LightCurveRevision) domain.LightCurveRevision {
//	cloned := revision
//
//	if revision.QualityPolicyVersion != nil {
//		qualityPolicyVersion := *revision.QualityPolicyVersion
//		cloned.QualityPolicyVersion = &qualityPolicyVersion
//	}
//
//	if revision.Epochs != nil {
//		cloned.Epochs = make(
//			[]domain.LightCurveEpoch,
//			len(revision.Epochs),
//		)
//		copy(cloned.Epochs, revision.Epochs)
//	}
//
//	return cloned
//}
