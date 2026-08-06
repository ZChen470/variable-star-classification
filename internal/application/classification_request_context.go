package application

import "context"

// classificationRequestIDContextKey 使用包内私有类型，避免与其他
// context value 的 key 发生冲突
type classificationRequestIDContextKey struct{}

// WithClassificationRequestID 将已验证的 Classification jon_id
// 保存为本次推理调用的 request ID
//
// Worker 在调用本函数前已经完成 command job_id 的确定性校验。
// 空 requestID 不覆盖已有 Context，nil Context 原样返回。
func WithClassificationRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil || requestID == "" {
		return ctx
	}

	return context.WithValue(ctx, classificationRequestIDContextKey{}, requestID)
}

// ClassificationRequestIDFromContext 读取本次分类推理的 request ID
func ClassificationRequestIDFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}

	requestID, ok := ctx.Value(classificationRequestIDContextKey{}).(string)
	if !ok || requestID == "" {
		return "", false
	}
	return requestID, true
}
