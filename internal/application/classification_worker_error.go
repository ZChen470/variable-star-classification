package application

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
)

// ClassificationWorkerErrorClass 表示 Worker 对错误采取的处理类别
type ClassificationWorkerErrorClass uint8

const (
	ClassificationWorkerErrorClassUnspecified ClassificationWorkerErrorClass = iota
	ClassificationWorkerErrorClassRetryable
	ClassificationWorkerErrorClassPermanent
	ClassificationWorkerErrorClassCancelled
)

func (class ClassificationWorkerErrorClass) String() string {
	switch class {
	case ClassificationWorkerErrorClassRetryable:
		return "RETRYABLE"
	case ClassificationWorkerErrorClassPermanent:
		return "PERMANENT"
	case ClassificationWorkerErrorClassCancelled:
		return "CANCELLED"
	default:
		return "UNSPECIFIED"
	}
}

// ClassificationWorkerOperation 标识错误发生在哪个 Worker 编排步骤。
type ClassificationWorkerOperation string

const (
	ClassificationWorkerOperationDecodeCommand ClassificationWorkerOperation = "decode-command"
	ClassificationWorkerOperationPrepareInput  ClassificationWorkerOperation = "prepare-input"
	ClassificationWorkerOperationResolveBundle ClassificationWorkerOperation = "resolve-serving-bundle"
	ClassificationWorkerOperationClassify      ClassificationWorkerOperation = "classify"
	ClassificationWorkerOperationBuildRun      ClassificationWorkerOperation = "build-run"
	ClassificationWorkerOperationBuildResult   ClassificationWorkerOperation = "build-result"
	ClassificationWorkerOperationPublishResult ClassificationWorkerOperation = "publish-result"
)

// ClassificationWorkerErrorCode 是后续 Command DLQ Header 使用的稳定错误代码
type ClassificationWorkerErrorCode string

const (
	ClassificationWorkerErrorCodeCancelled ClassificationWorkerErrorCode = "WORKER_CANCELLED"

	ClassificationWorkerErrorCodeLightCurveNotFound    ClassificationWorkerErrorCode = "LIGHT_CURVE_NOT_FOUND"
	ClassificationWorkerErrorCodeLightCurveNotReady    ClassificationWorkerErrorCode = "LIGHT_CURVE_NOT_READY"
	ClassificationWorkerErrorCodeLightCurveInvalid     ClassificationWorkerErrorCode = "LIGHT_CURVE_INVALID"
	ClassificationWorkerErrorCodeLightCurveUnavailable ClassificationWorkerErrorCode = "LIGHT_CURVE_UNAVAILABLE"

	ClassificationWorkerErrorCodeModelBundleNotFound ClassificationWorkerErrorCode = "MODEL_BUNDLE_NOT_FOUND"
	ClassificationWorkerErrorCodeModelBundleInvalid  ClassificationWorkerErrorCode = "MODEL_BUNDLE_INVALID"

	ClassificationWorkerErrorCodeTritonRequestRejected ClassificationWorkerErrorCode = "TRITON_REQUEST_REJECTED"
	ClassificationWorkerErrorCodeTritonUnavailable     ClassificationWorkerErrorCode = "TRITON_UNAVAILABLE"

	ClassificationWorkerErrorCodeResultInvalid ClassificationWorkerErrorCode = "CLASSIFICATION_RESULT_INVALID"

	ClassificationWorkerErrorCodeDependencyUnavailable ClassificationWorkerErrorCode = "DEPENDENCY_UNAVAILABLE"
	ClassificationWorkerErrorCodePublishFailed         ClassificationWorkerErrorCode = "RESULT_PUBLISH_FAILED"
	ClassificationWorkerErrorCodeInternalInvalid       ClassificationWorkerErrorCode = "WORKER_INTERNAL_INVALID"
)

// ClassificationWorkerError 保存稳定分类信息，同时通过 Unwrap 保留原始 Cause
type ClassificationWorkerError struct {
	Code      ClassificationWorkerErrorCode
	Class     ClassificationWorkerErrorClass
	Operation ClassificationWorkerOperation
	Cause     error
}

func (workerError *ClassificationWorkerError) Error() string {
	if workerError == nil {
		return "nil classification worker error"
	}

	if workerError.Cause == nil {
		return fmt.Sprintf("%s [%s] during %s", workerError.Code, workerError.Class, workerError.Operation)
	}

	return fmt.Sprintf(
		"%s [%s] during %s: %v",
		workerError.Code,
		workerError.Class,
		workerError.Operation,
		workerError.Cause,
	)
}

func (workerError *ClassificationWorkerError) Unwrap() error {
	if workerError == nil {
		return nil
	}

	return workerError.Cause
}

// ClassificationWorkerClassifiedCause 允许基础设施 Adapter 使用稳定类型
// 显式声明某个错误的 Worker 分类，而不要求 application 反向依赖 Adapter。
type ClassificationWorkerClassifiedCause interface {
	error

	ClassificationWorkerClass() ClassificationWorkerErrorClass
	ClassificationWorkerCode() ClassificationWorkerErrorCode
}

// classificationWorkerHTTPStatusCause 由 HTTP Adapter 错误实现。
// application 不需要导入具体 Triton Adapter
type classificationWorkerHTTPStatusCause interface {
	error
	HTTPStatusCode() int
}

// WrapClassificationWorkerError 将某个编排步骤产生的 Cause 转换为稳定 WorkerError
//
// 原始 Cause 始终通过 Unwrap 保留，因此 errors.Is / errors.As 继续有效
func WrapClassificationWorkerError(operation ClassificationWorkerOperation, cause error) error {
	if cause == nil {
		return nil
	}

	var existing *ClassificationWorkerError
	if errors.As(cause, &existing) {
		return cause
	}

	class, code := classifyClassificationWorkerCause(operation, cause)

	return &ClassificationWorkerError{
		Code:      code,
		Class:     class,
		Operation: operation,
		Cause:     cause,
	}
}

func classifyClassificationWorkerCause(operation ClassificationWorkerOperation, cause error) (
	ClassificationWorkerErrorClass,
	ClassificationWorkerErrorCode,
) {
	if errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded) {
		return ClassificationWorkerErrorClassCancelled,
			ClassificationWorkerErrorCodeCancelled
	}

	var classified ClassificationWorkerClassifiedCause
	if errors.As(cause, &classified) {
		return classified.ClassificationWorkerClass(),
			classified.ClassificationWorkerCode()
	}

	var commandError *PermanentClassificationCommandError
	if errors.As(cause, &commandError) {
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCode(commandError.Code)
	}

	switch {
	case errors.Is(cause, ErrLightCurveRevisionNotFound):
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCodeLightCurveNotFound

	case errors.Is(cause, ErrLightCurveRevisionNotReady):
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodeLightCurveNotReady

	case errors.Is(cause, ErrLightCurveSourceUnavailable):
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodeLightCurveUnavailable

	case errors.Is(cause, ErrLightCurveRevisionInconsistent),
		errors.Is(cause, ErrLightCurveRevisionIdentityMismatch),
		errors.Is(cause, ErrLightCurveEpochCountMismatch),
		errors.Is(cause, ErrInsufficientLightCurveEpochs),
		errors.Is(cause, ErrTooManyLightCurveEpochs),
		errors.Is(cause, ErrInvalidObservationTime),
		errors.Is(cause, ErrInvalidMagnitude),
		errors.Is(cause, ErrInvalidMagnitudeError),
		errors.Is(cause, ErrDuplicateObservationTime):
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCodeLightCurveInvalid

	case errors.Is(cause, ErrModelBundleNotFound),
		errors.Is(cause, ErrServingBundleNotFound):
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCodeModelBundleNotFound

	case errors.Is(cause, ErrInvalidModelBundleMetadata),
		errors.Is(cause, ErrInvalidCompatibleCoarseResult),
		errors.Is(cause, ErrInvalidCoarseModeSelection):
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCodeModelBundleInvalid

	case errors.Is(cause, ErrInvalidClassificationRunBuild),
		errors.Is(cause, ErrInvalidClassificationResultMessage):
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCodeResultInvalid
	}

	var statusCause classificationWorkerHTTPStatusCause
	if errors.As(cause, &statusCause) {
		return classifyClassificationWorkerHTTPStatus(
			statusCause.HTTPStatusCode(),
		)
	}

	var networkError net.Error
	if errors.As(cause, &networkError) {
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodeDependencyUnavailable
	}

	// 对无法精确识别的依赖故障采取保守重试；
	// 对纯内部构造步骤采取永久失败，避免无限重试确定性 Bug。
	switch operation {
	case ClassificationWorkerOperationPrepareInput,
		ClassificationWorkerOperationResolveBundle,
		ClassificationWorkerOperationClassify:
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodeDependencyUnavailable

	case ClassificationWorkerOperationPublishResult:
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodePublishFailed

	default:
		return ClassificationWorkerErrorClassPermanent,
			ClassificationWorkerErrorCodeInternalInvalid
	}
}

func classifyClassificationWorkerHTTPStatus(
	statusCode int,
) (
	ClassificationWorkerErrorClass,
	ClassificationWorkerErrorCode,
) {
	switch statusCode {
	case http.StatusRequestTimeout,
		http.StatusTooEarly,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodeTritonUnavailable
	}

	if statusCode >= 500 && statusCode <= 599 {
		return ClassificationWorkerErrorClassRetryable,
			ClassificationWorkerErrorCodeTritonUnavailable
	}

	return ClassificationWorkerErrorClassPermanent,
		ClassificationWorkerErrorCodeTritonRequestRejected
}
