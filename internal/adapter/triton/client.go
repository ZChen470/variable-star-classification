package triton

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

var (
	ErrNilContext                 = errors.New("context must not be nil")
	ErrInvalidClientConfiguration = errors.New("invalid Triton HTTP client configuration")
	ErrInvalidModelRequest        = errors.New("invalid Triton model request")
	ErrResponseTooLarge           = errors.New("Triton HTTP response exceeds size limit")
)

var (
	modelNamePattern = regexp.MustCompile(
		`^[A-Za-z0-9][A-Za-z0-9_.-]*$`,
	)

	modelVersionPattern = regexp.MustCompile(
		`^[1-9][0-9]*$`,
	)
)

// ModelOperation 表示 Triton v2 HTTP 的版本化模型操作
type ModelOperation uint8

const (
	ModelOperationUnspecified ModelOperation = iota

	ModelOperationMetadata
	ModelOperationConfig
	ModelOperationReady
	ModelOperationInfer
)

// ModelRequest 是一个精确 model name / version 的 Triton 请求
type ModelRequest struct {
	ModelName    string
	ModelVersion string
	Operation    ModelOperation

	Header http.Header
	Body   []byte
}

// ModelResponse 保存 Triton HTTP 响应
// Body 已完整读入，并受到 Client 的大小限制
type ModelResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// HTTPStatusError 表示 Triton 返回了非 2xx 的状态码
type HTTPStatusError struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (err *HTTPStatusError) Error() string {
	if err == nil {
		return "nil Triton HTTP status error"
	}

	return fmt.Sprintf("Triton HTTP response status %d", err.StatusCode)
}

// HTTPStatusCode 允许应用层错误分类器读取稳定 HTTP 状态
// 而不要求 application 依赖具体 Triton Adapter 类型
func (err *HTTPStatusError) HTTPStatusCode() int {
	if err == nil {
		return 0
	}
	return err.StatusCode
}

// Client 是通用的 Triton v2 HTTP Client
type Client struct {
	baseURL         string
	httpClient      *http.Client
	maxResponseSize int64
}

// NewClient 创建 Triton v2 HTTP Client
//
// baseURL 示例：http://127.0.0.1:8000
// httpClient 应由装配曾配置 timeout
// maxResponseSize 是响应体允许的最大字节数
func NewClient(baseURL string, httpClient *http.Client, maxResponseSize int64) (*Client, error) {
	if strings.TrimSpace(baseURL) != baseURL || baseURL == "" {
		return nil, fmt.Errorf("%w: base URL is empty or contains surrounding whitespace", ErrInvalidClientConfiguration)
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse base URL: %v", ErrInvalidClientConfiguration, err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: unsupported base URL scheme %q", ErrInvalidClientConfiguration, parsed.Scheme)
	}

	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: base URL host is empty", ErrInvalidClientConfiguration)
	}

	if parsed.User != nil {
		return nil, fmt.Errorf("%w: base URL must not contain user information", ErrInvalidClientConfiguration)
	}

	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: base URL must not contain query or fragment", ErrInvalidClientConfiguration)
	}

	if httpClient == nil {
		return nil, fmt.Errorf("%w: HTTP client is nil", ErrInvalidClientConfiguration)
	}
	if maxResponseSize <= 0 {
		return nil, fmt.Errorf("%w: max response size must be positive", ErrInvalidClientConfiguration)
	}

	return &Client{
		baseURL:         baseURL,
		httpClient:      httpClient,
		maxResponseSize: maxResponseSize,
	}, nil
}

// Do 执行一个精确版本的 Triton 模型请求
func (client *Client) Do(ctx context.Context, modelRequest ModelRequest) (ModelResponse, error) {
	if ctx == nil {
		return ModelResponse{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}

	if client == nil {
		return ModelResponse{}, fmt.Errorf("%w: client is not initialized", ErrInvalidModelRequest)
	}

	method, suffix, err := validateModelRequest(modelRequest)
	if err != nil {
		return ModelResponse{}, err
	}

	endpoint, err := url.JoinPath(client.baseURL, "v2", "models", modelRequest.ModelName, "versions", modelRequest.ModelVersion)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("build Triton model endpoint: %w", err)
	}
	if suffix != "" {
		endpoint, err = url.JoinPath(endpoint, strings.TrimPrefix(suffix, "/"))
		if err != nil {
			return ModelResponse{}, fmt.Errorf("build Triton operation endpoint: %w", err)
		}
	}

	var requestBody io.Reader
	if len(modelRequest.Body) > 0 {
		requestBody = bytes.NewReader(modelRequest.Body)
	}

	request, err := http.NewRequestWithContext(ctx, method, endpoint, requestBody)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("create Triton HTTP request: %w", err)
	}

	request.Header = modelRequest.Header.Clone()

	httpResponse, err := client.httpClient.Do(request)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("execute Triton HTTP request: %w", err)
	}

	if httpResponse == nil {
		return ModelResponse{}, fmt.Errorf("execute Triton HTTP request: nil response")
	}
	defer httpResponse.Body.Close()

	body, err := io.ReadAll(io.LimitReader(httpResponse.Body, client.maxResponseSize+1))
	if err != nil {
		return ModelResponse{}, fmt.Errorf("read Triton HTTP response: %w", err)
	}

	if int64(len(body)) > client.maxResponseSize {
		return ModelResponse{}, fmt.Errorf("%w: limit=%d", ErrResponseTooLarge, client.maxResponseSize)
	}

	response := ModelResponse{
		StatusCode: httpResponse.StatusCode,
		Header:     httpResponse.Header,
		Body:       body,
	}

	if httpResponse.StatusCode < http.StatusOK || httpResponse.StatusCode >= http.StatusMultipleChoices {
		return response, &HTTPStatusError{
			StatusCode: response.StatusCode,
			Header:     response.Header.Clone(),
			Body:       append([]byte{}, response.Body...),
		}
	}

	return response, nil
}

func validateModelRequest(request ModelRequest) (string, string, error) {
	if !modelNamePattern.MatchString(request.ModelName) {
		return "", "", fmt.Errorf("%w: invalid model name %q", ErrInvalidModelRequest, request.ModelName)
	}

	if !modelVersionPattern.MatchString(request.ModelVersion) {
		return "", "", fmt.Errorf("%w: invalid model version %q", ErrInvalidModelRequest, request.ModelVersion)
	}

	var (
		method string
		suffix string
	)

	switch request.Operation {
	case ModelOperationMetadata:
		method = http.MethodGet
	case ModelOperationConfig:
		method = http.MethodGet
		suffix = "/config"
	case ModelOperationReady:
		method = http.MethodGet
		suffix = "/ready"
	case ModelOperationInfer:
		method = http.MethodPost
		suffix = "/infer"
	default:
		return "", "", fmt.Errorf("%w: unsupported operation %d", ErrInvalidModelRequest, request.Operation)
	}

	if method == http.MethodGet && len(request.Body) != 0 {
		return "", "", fmt.Errorf("%w: GET operation must not contain a body", ErrInvalidModelRequest)
	}

	return method, suffix, nil
}
