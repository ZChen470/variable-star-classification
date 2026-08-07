package lightcurve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var (
	ErrInvalidRepositoryConfiguration = errors.New("invalid light curve HTTP repository configuration")
	ErrInvalidRevisionRequest         = errors.New("invalid light curve revision request")
)

// Repository 通过上游 LightCurveRevision HTTP 数据服务
// 精确读取固定 object_id + light_curve_revision
//
// 本 Adapter 不执行：
//   - latest fallback
//   - epoch 数量范围检查
//   - epoch 数值合法性校验
//   - epoch 排序：
//   - 重复 ObservationTime 检查
//   - 科学质量策略判断
type Repository struct {
	baseURL    string
	httpClient *http.Client
}

// NewRepository 创建真实 LightCurveRevision HTTP Repository
//
// httpClient 的 Timeout、Transport、认证以及 telemetry
// 由 composition root 负责
func NewRepository(baseURL string, httpClient *http.Client) (*Repository, error) {
	if baseURL == "" || strings.TrimSpace(baseURL) != baseURL {
		return nil, fmt.Errorf("%w: base URL is empty or contains surrounding whitesapce", ErrInvalidRepositoryConfiguration)
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse base URL： %v", ErrInvalidRepositoryConfiguration, err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: unsupported base URL scheme %q", ErrInvalidRepositoryConfiguration, parsed.Scheme)
	}

	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: base URL host is empty", ErrInvalidRepositoryConfiguration)
	}

	if httpClient == nil {
		return nil, fmt.Errorf("%w: HTTP client is nil", ErrInvalidRepositoryConfiguration)
	}

	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, fmt.Errorf(
			"%w: base URL must not contain query or fragment",
			ErrInvalidRepositoryConfiguration,
		)
	}

	return &Repository{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
	}, nil
}

// GetRevision 精确读取固定 revision
//
// objectID 不会被 trim 或改写，只会进行 URL path escaping
// revision 不会被替换为 latest
func (repository *Repository) GetRevision(ctx context.Context, objectID string, revision int64) (domain.LightCurveRevision, error) {
	if ctx == nil {
		return domain.LightCurveRevision{}, errors.New("get light curve revision: nil context")
	}
	if err := ctx.Err(); err != nil {
		return domain.LightCurveRevision{}, err
	}

	if repository == nil {
		return domain.LightCurveRevision{}, fmt.Errorf("%w: repository is not initialized", ErrInvalidRepositoryConfiguration)
	}

	if objectID == "" {
		return domain.LightCurveRevision{}, fmt.Errorf("%w, object_id must not be empty", ErrInvalidRevisionRequest)
	}
	if revision <= 0 {
		return domain.LightCurveRevision{}, fmt.Errorf("%w: light_curve_revision must greater than zero", ErrInvalidRevisionRequest)
	}

	endpoint := repository.baseURL + "/internal/v1/objects/" + url.PathEscape(objectID) + "/light-curves/" + strconv.FormatInt(revision, 10)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.LightCurveRevision{}, fmt.Errorf("%w: create HTTP request: %v", ErrInvalidRevisionRequest, err)
	}

	request.Header.Set("Accept", "application/json")

	response, err := repository.httpClient.Do(request)
	if err != nil {
		// 保留原始 context 错误链
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return domain.LightCurveRevision{}, fmt.Errorf("%w: execute HTTP request: %w", application.ErrLightCurveSourceUnavailable, err)
		}

		return domain.LightCurveRevision{}, fmt.Errorf("%w: execute HTTP request: %w", application.ErrLightCurveSourceUnavailable, err)
	}
	if response == nil {
		return domain.LightCurveRevision{}, fmt.Errorf("%w: HTTP client returned nil response", application.ErrLightCurveSourceUnavailable)
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		drainLightCurveErrorBody(response.Body)
		return domain.LightCurveRevision{}, mapLightCurveHTTPStatus(response.StatusCode)
	}

	payload, err := decodeLightCurveRevisionResponse(response.Body)
	if err != nil {
		return domain.LightCurveRevision{}, err
	}

	return mapLightCurveRevisionResponse(payload, objectID, revision)
}

func mapLightCurveHTTPStatus(statusCode int) error {
	switch statusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%w: HTTP status %d", application.ErrLightCurveRevisionNotFound, statusCode)

	case http.StatusConflict:
		return fmt.Errorf("%w: HTTP status %d", application.ErrLightCurveRevisionNotReady, statusCode)

	case http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: HTTP status %d", application.ErrLightCurveRevisionInconsistent, statusCode)

	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable:
		return fmt.Errorf("%w: HTTP status %d", application.ErrLightCurveSourceUnavailable, statusCode)

	default:
		// 未冻结的非 200 状态不擅自赋予新的永久业务语义。
		// 当前只能确认上游未能提供可接受的固定 revision，
		// 因而保守归类为 SourceUnavailable。
		return fmt.Errorf("%w: unexpected HTTP status %d", application.ErrLightCurveSourceUnavailable, statusCode)
	}
}

func drainLightCurveErrorBody(body io.Reader) {
	if body == nil {
		return
	}

	// 只做有限 drain，帮助 HTTP keep-alive 复用，
	// 不把上游错误 Body 纳入稳定业务契约。
	_, _ = io.Copy(
		io.Discard,
		io.LimitReader(
			body,
			4096,
		),
	)
}

type lightCurveRevisionResponse struct {
	ObjectID             *string                    `json:"object_id"`
	LightCurveRevision   *int64                     `json:"light_curve_revision"`
	EligibleEpochCount   *uint32                    `json:"eligible_epoch_count"`
	QualityPolicyVersion *string                    `json:"quality_policy_version"`
	Epochs               *[]lightCurveEpochResponse `json:"epochs"`
}

type lightCurveEpochResponse struct {
	ObservationTime *float64 `json:"observation_time"`
	Magnitude       *float32 `json:"magnitude"`
	MagnitudeError  *float32 `json:"magnitude_error"`
}

func decodeLightCurveRevisionResponse(body io.Reader) (lightCurveRevisionResponse, error) {
	if body == nil {
		return lightCurveRevisionResponse{}, fmt.Errorf("%w: response body is nil", application.ErrLightCurveRevisionInconsistent)
	}

	decoder := json.NewDecoder(body)

	// 上游契约定义的是“最小字段”，因此这里有意允许未知字段，
	// 为上游向后兼容地增加字段保留空间。
	var payload lightCurveRevisionResponse

	if err := decoder.Decode(&payload); err != nil {
		return lightCurveRevisionResponse{}, fmt.Errorf("%w: decode HTTP response JSON: %v", application.ErrLightCurveRevisionInconsistent, err)
	}

	var trailing any

	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return lightCurveRevisionResponse{}, fmt.Errorf("%w: multiple JSON values are not allowed", application.ErrLightCurveRevisionInconsistent)
		}

		return lightCurveRevisionResponse{}, fmt.Errorf("%w: decode trailing JSON: %v", application.ErrLightCurveRevisionInconsistent, err)
	}

	if payload.ObjectID == nil {
		return lightCurveRevisionResponse{}, missingLightCurveResponseField("object_id")
	}

	if payload.LightCurveRevision == nil {
		return lightCurveRevisionResponse{}, missingLightCurveResponseField("light_curve_revision")
	}

	if payload.EligibleEpochCount == nil {
		return lightCurveRevisionResponse{}, missingLightCurveResponseField("eligible_epoch_count")
	}

	if payload.Epochs == nil {
		return lightCurveRevisionResponse{}, missingLightCurveResponseField("epochs")
	}

	for index, epoch := range *payload.Epochs {
		switch {
		case epoch.ObservationTime == nil:
			return lightCurveRevisionResponse{},
				missingLightCurveEpochField(index, "observation_time")

		case epoch.Magnitude == nil:
			return lightCurveRevisionResponse{},
				missingLightCurveEpochField(index, "magnitude")

		case epoch.MagnitudeError == nil:
			return lightCurveRevisionResponse{},
				missingLightCurveEpochField(index, "magnitude_error")
		}
	}

	return payload, nil
}

func mapLightCurveRevisionResponse(payload lightCurveRevisionResponse, requestedObjectID string, requestedRevision int64) (domain.LightCurveRevision, error) {
	if *payload.ObjectID != requestedObjectID {
		return domain.LightCurveRevision{}, fmt.Errorf("%w: requested object_id=%q, returned object_id=%q", application.ErrLightCurveRevisionInconsistent, requestedObjectID, *payload.ObjectID)
	}

	if *payload.LightCurveRevision !=
		requestedRevision {
		return domain.LightCurveRevision{}, fmt.Errorf("%w: requested revision=%d, returned revision=%d", application.ErrLightCurveRevisionInconsistent, requestedRevision, *payload.LightCurveRevision)
	}

	epochs := make([]domain.LightCurveEpoch, len(*payload.Epochs))

	for index, epoch := range *payload.Epochs {
		epochs[index] =
			domain.LightCurveEpoch{
				ObservationTime: *epoch.ObservationTime,

				Magnitude: *epoch.Magnitude,

				MagnitudeError: *epoch.MagnitudeError,
			}
	}

	var qualityPolicyVersion *string

	if payload.QualityPolicyVersion != nil {
		copied := *payload.QualityPolicyVersion
		qualityPolicyVersion = &copied
	}

	return domain.LightCurveRevision{
		ObjectID:             *payload.ObjectID,
		Revision:             *payload.LightCurveRevision,
		EligibleEpochCount:   *payload.EligibleEpochCount,
		QualityPolicyVersion: qualityPolicyVersion,
		Epochs:               epochs,
	}, nil
}

func missingLightCurveResponseField(field string) error {
	return fmt.Errorf("%w: required response field %q is missing", application.ErrLightCurveRevisionInconsistent, field)
}

func missingLightCurveEpochField(index int, field string) error {
	return fmt.Errorf(
		"%w: epochs[%d].%s is missing",
		application.ErrLightCurveRevisionInconsistent,
		index,
		field,
	)
}
