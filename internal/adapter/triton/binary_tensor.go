package triton

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strconv"
)

const (
	BinaryContentType            = "application/octet-stream"
	InferenceHeaderContentLength = "Inference-Header-Content-Length"

	maxInt = int(^uint(0) >> 1)
)

var (
	ErrInvalidBinaryTensor        = errors.New("invalid Triton binary tensor")
	ErrInvalidBinaryInferRequest  = errors.New("invalid Triton binary inference request")
	ErrInvalidBinaryInferResponse = errors.New("invalid Triton binary inference response")
)

// TensorDataType 是当前项目使用的 Triton Tensor 类别
type TensorDataType string

const (
	TensorDataTypeFP64  TensorDataType = "FP64"
	TensorDataTypeFP32  TensorDataType = "FP32"
	TensorDataTypeINT32 TensorDataType = "INT32"
	TensorDataTypeBOOL  TensorDataType = "BOOL"
)

// BinaryTensor 保存一个已经编码为 Triton 二进制格式的 Tensor
type BinaryTensor struct {
	Name     string
	DataType TensorDataType
	Shape    []int64
	Data     []byte
}

// BinaryInferResponse 是解码后的 Triton 推理响应
type BinaryInferResponse struct {
	ID           string
	ModelName    string
	ModelVersion string
	Outputs      []BinaryTensor
}

type binaryInferRequestHeader struct {
	ID      string                    `json:"id,omitempty"`
	Inputs  []binaryInferInputHeader  `json:"inputs"`
	Outputs []binaryInferOutputHeader `json:"outputs"`
}

type binaryInferInputHeader struct {
	Name       string                `json:"name"`
	Shape      []int64               `json:"shape"`
	DataType   TensorDataType        `json:"datatype"`
	Parameters binaryInputParameters `json:"parameters"`
}

type binaryInferOutputHeader struct {
	Name       string                 `json:"name"`
	Parameters binaryOutputParameters `json:"parameters"`
}

type binaryInputParameters struct {
	BinaryDataSize int `json:"binary_data_size"`
}

type binaryOutputParameters struct {
	BinaryData bool `json:"binary_data"`
}

type binaryInferResponseHeader struct {
	ID           string                            `json:"id,omitempty"`
	ModelName    string                            `json:"model_name"`
	ModelVersion string                            `json:"model_version"`
	Outputs      []binaryInferResponseOutputHeader `json:"outputs"`
}

type binaryInferResponseOutputHeader struct {
	Name       string                   `json:"name"`
	Shape      []int64                  `json:"shape"`
	DataType   TensorDataType           `json:"datatype"`
	Parameters binaryResponseParameters `json:"parameters"`
}

type binaryResponseParameters struct {
	BinaryDataSize *int64 `json:"binary_data_size"`
}

func NewFP64Tensor(name string, shape []int64, values []float64) (BinaryTensor, error) {
	data := make([]byte, len(values)*8)

	for i, v := range values {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(v))
	}

	return newBinaryTensor(name, TensorDataTypeFP64, shape, data)
}

func NewFP32Tensor(name string, shape []int64, values []float32) (BinaryTensor, error) {
	data := make([]byte, len(values)*4)

	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(v))
	}

	return newBinaryTensor(name, TensorDataTypeFP32, shape, data)
}

func NewINT32Tensor(name string, shape []int64, values []int32) (BinaryTensor, error) {
	data := make([]byte, len(values)*4)

	for i, v := range values {
		binary.LittleEndian.PutUint32(data[i*4:], uint32(v))
	}

	return newBinaryTensor(name, TensorDataTypeINT32, shape, data)
}

func NewBOOLTensor(name string, shape []int64, values []bool) (BinaryTensor, error) {
	data := make([]byte, len(values))

	for i, v := range values {
		if v {
			data[i] = 1
		}
	}

	return newBinaryTensor(name, TensorDataTypeBOOL, shape, data)
}

func newBinaryTensor(name string, dataType TensorDataType, shape []int64, data []byte) (BinaryTensor, error) {
	tensor := BinaryTensor{
		Name:     name,
		DataType: dataType,
		Shape:    append([]int64{}, shape...),
		Data:     append([]byte{}, data...),
	}

	if err := validateBinaryTensor(tensor); err != nil {
		return BinaryTensor{}, err
	}

	return tensor, nil
}

func validateBinaryTensor(tensor BinaryTensor) error {
	if !modelNamePattern.MatchString(tensor.Name) {
		return fmt.Errorf("%w: invalid name %q", ErrInvalidBinaryTensor, tensor.Name)
	}

	dataSize, err := expectedTensorDataSize(tensor.DataType, tensor.Shape)
	if err != nil {
		return fmt.Errorf("%w: tensor %q: %v", ErrInvalidBinaryTensor, tensor.Name, err)
	}

	if len(tensor.Data) != dataSize {
		return fmt.Errorf("%w: tensor %q data size=%d, want %d", ErrInvalidBinaryTensor, tensor.Name, len(tensor.Data), dataSize)
	}

	if tensor.DataType == TensorDataTypeBOOL {
		for index, value := range tensor.Data {
			if value != 0 && value != 1 {
				return fmt.Errorf("%w: tensor %q BOOL byte[%d]=%d", ErrInvalidBinaryTensor, tensor.Name, index, value)
			}
		}
	}

	return nil
}

func requireTensorType(tensor BinaryTensor, want TensorDataType) error {
	if err := validateBinaryTensor(tensor); err != nil {
		return err
	}
	if tensor.DataType != want {
		return fmt.Errorf("%w: tensor %q datatype=%q, want %q", ErrInvalidBinaryTensor, tensor.Name, tensor.DataType, want)
	}
	return nil
}

func expectedTensorDataSize(dataType TensorDataType, shape []int64) (int, error) {
	elementSize, err := tensorElementSize(dataType)
	if err != nil {
		return 0, err
	}

	elementCount, err := tensorElementCount(shape)
	if err != nil {
		return 0, err
	}

	if elementCount > maxInt/elementSize {
		return 0, errors.New("tensor byte size overflows int")
	}

	return elementCount * elementSize, nil
}

func tensorElementSize(dataType TensorDataType) (int, error) {
	switch dataType {
	case TensorDataTypeFP64:
		return 8, nil
	case TensorDataTypeFP32, TensorDataTypeINT32:
		return 4, nil
	case TensorDataTypeBOOL:
		return 1, nil
	default:
		return 0, fmt.Errorf("unsupported datatype %q", dataType)
	}
}

func tensorElementCount(shape []int64) (int, error) {
	if len(shape) == 0 {
		return 0, errors.New("shape is empty")
	}

	count := int64(1)
	maximum := int64(maxInt)

	for i, dimension := range shape {
		if dimension <= 0 {
			return 0, fmt.Errorf("shape[%d]=%d must be positive", i, dimension)
		}
		if count > maximum/dimension {
			return 0, errors.New("tensor element count overflow int")
		}
		count *= dimension
	}

	return int(count), nil
}

// EncodeBinaryInferRequest 构造可直接交给 client.Do 的请求
// 输入 Tensor 按 inputs 顺序拼接，输出按 outputNames 显式请求二进制结果
func EncodeBinaryInferRequest(modelName string, modelVersion string, requestID string, inputs []BinaryTensor, outputNames []string) (ModelRequest, error) {
	if len(inputs) == 0 {
		return ModelRequest{}, fmt.Errorf("%w: no input tensors", ErrInvalidBinaryInferRequest)
	}
	if len(outputNames) == 0 {
		return ModelRequest{}, fmt.Errorf("%w: no requested outputs", ErrInvalidBinaryInferRequest)
	}

	header := binaryInferRequestHeader{
		ID:      requestID,
		Inputs:  make([]binaryInferInputHeader, len(inputs)),
		Outputs: make([]binaryInferOutputHeader, len(outputNames)),
	}

	// 用于给 tensor 按 name 去重
	inputNames := make(map[string]struct{}, len(inputs))
	payloadSize := 0

	for i, tensor := range inputs {
		if err := validateBinaryTensor(tensor); err != nil {
			return ModelRequest{}, fmt.Errorf("%w: input %d: %w", ErrInvalidBinaryInferRequest, i, err)
		}
		if _, exists := inputNames[tensor.Name]; exists {
			return ModelRequest{}, fmt.Errorf("%w: duplicate input %q", ErrInvalidBinaryInferRequest, tensor.Name)
		}
		inputNames[tensor.Name] = struct{}{}

		if len(tensor.Data) > maxInt-payloadSize {
			return ModelRequest{}, fmt.Errorf("%w: binary payload is too large", ErrInvalidBinaryInferRequest)
		}
		payloadSize += len(tensor.Data)

		header.Inputs[i] = binaryInferInputHeader{
			Name:     tensor.Name,
			Shape:    append([]int64{}, tensor.Shape...),
			DataType: tensor.DataType,
			Parameters: binaryInputParameters{
				BinaryDataSize: len(tensor.Data),
			},
		}
	}

	outputSet := make(map[string]struct{}, len(outputNames))
	for i, name := range outputNames {
		if !modelNamePattern.MatchString(name) {
			return ModelRequest{}, fmt.Errorf("%w: invalid output name %q", ErrInvalidBinaryInferRequest, name)
		}
		if _, exists := outputSet[name]; exists {
			return ModelRequest{}, fmt.Errorf("%w: duplicate output %q", ErrInvalidBinaryInferRequest, name)
		}

		outputSet[name] = struct{}{}

		header.Outputs[i] = binaryInferOutputHeader{
			Name: name,
			Parameters: binaryOutputParameters{
				BinaryData: true,
			},
		}
	}

	// 序列化时只需要序列化 header 部分
	jsonHeader, err := json.Marshal(header)
	if err != nil {
		return ModelRequest{}, fmt.Errorf("%w: encode JSON header: %v", ErrInvalidBinaryInferRequest, err)
	}

	if len(jsonHeader) > maxInt-payloadSize {
		return ModelRequest{}, fmt.Errorf("%w, request body is too large", ErrInvalidBinaryInferRequest)
	}

	body := make([]byte, 0, len(jsonHeader)+payloadSize)
	body = append(body, jsonHeader...)

	for _, tensor := range inputs {
		body = append(body, tensor.Data...)
	}

	request := ModelRequest{
		ModelName:    modelName,
		ModelVersion: modelVersion,
		Operation:    ModelOperationInfer,
		Header: http.Header{
			"Content-Type":               []string{BinaryContentType},
			InferenceHeaderContentLength: []string{strconv.Itoa(len(jsonHeader))},
		},
		Body: body,
	}

	if _, _, err = validateModelRequest(request); err != nil {
		return ModelRequest{}, fmt.Errorf("%w, %w", ErrInvalidBinaryInferRequest, err)
	}
	return request, nil
}

// DecodeBinaryInferResponse 按响应 JSON 中 outputs 的顺序解码
// 不假设 Triton 按客户端预设顺序返回 Tensor
func DecodeBinaryInferResponse(response ModelResponse) (BinaryInferResponse, error) {
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return BinaryInferResponse{}, fmt.Errorf("%w: HTTP status %d", ErrInvalidBinaryInferResponse, response.StatusCode)
	}

	contentType := response.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != BinaryContentType {
		return BinaryInferResponse{}, fmt.Errorf("%w: Content-Type=%q", ErrInvalidBinaryInferResponse, contentType)
	}

	headerLengthText := response.Header.Get(InferenceHeaderContentLength)
	headerLength, err := strconv.ParseInt(headerLengthText, 10, 64)
	if err != nil || headerLength <= 0 {
		return BinaryInferResponse{}, fmt.Errorf("%w: invalid %s=%q", ErrInvalidBinaryInferResponse, InferenceHeaderContentLength, headerLength)
	}

	if headerLength > int64(len(response.Body)) || headerLength > int64(maxInt) {
		return BinaryInferResponse{}, fmt.Errorf("%w: JSON header length %d exceeds body length %d", ErrInvalidBinaryInferResponse, headerLength, len(response.Body))
	}

	var header binaryInferResponseHeader
	// 解码响应的 header 部分
	if err = decodeSingleJSON(response.Body[:int(headerLength)], &header); err != nil {
		return BinaryInferResponse{}, fmt.Errorf("%w: decode JSON header: %v", ErrInvalidBinaryInferResponse, err)
	}

	if !modelNamePattern.MatchString(header.ModelName) {
		return BinaryInferResponse{}, fmt.Errorf("%w: invalid model_name %q", ErrInvalidBinaryInferResponse, header.ModelName)
	}
	if !modelVersionPattern.MatchString(header.ModelVersion) {
		return BinaryInferResponse{}, fmt.Errorf("%w: invalid model_version %q", ErrInvalidBinaryInferResponse, header.ModelVersion)
	}
	if len(header.Outputs) == 0 {
		return BinaryInferResponse{}, fmt.Errorf("%w: no output tensors", ErrInvalidBinaryInferResponse)
	}

	decoded := BinaryInferResponse{
		ID:           header.ID,
		ModelName:    header.ModelName,
		ModelVersion: header.ModelVersion,
		Outputs:      make([]BinaryTensor, 0, len(header.Outputs)),
	}

	offset := int(headerLength)
	for i, output := range header.Outputs {
		if !modelNamePattern.MatchString(output.Name) {
			return BinaryInferResponse{}, fmt.Errorf("%w: invalid output name %q", ErrInvalidBinaryInferResponse, output.Name)
		}

		if output.Parameters.BinaryDataSize == nil || *output.Parameters.BinaryDataSize <= 0 {
			return BinaryInferResponse{}, fmt.Errorf("%w: output %q has invalid binary_data_size", ErrInvalidBinaryInferResponse, output.Name)
		}

		dataSize := *output.Parameters.BinaryDataSize
		if dataSize > int64(maxInt) || offset > len(response.Body)-int(dataSize) {
			return BinaryInferResponse{}, fmt.Errorf("%w: output %q binary data exceeds response body", ErrInvalidBinaryInferResponse, output.Name)
		}

		end := offset + int(dataSize)

		tensor := BinaryTensor{
			Name:     output.Name,
			DataType: output.DataType,
			Shape:    append([]int64{}, output.Shape...),
			Data:     response.Body[offset:end],
		}

		if err := validateBinaryTensor(tensor); err != nil {
			return BinaryInferResponse{}, fmt.Errorf("%w: output %d: %w", ErrInvalidBinaryInferResponse, i, err)
		}

		decoded.Outputs = append(decoded.Outputs, tensor)
		offset = end
	}

	if offset != len(response.Body) {
		return BinaryInferResponse{}, fmt.Errorf("%w: %d trailing binary bytes", ErrInvalidBinaryInferResponse, len(response.Body)-offset)
	}

	return decoded, nil
}

func decodeSingleJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}

	return nil
}

func DecodeFP64Values(tensor BinaryTensor) ([]float64, error) {
	if err := requireTensorType(tensor, TensorDataTypeFP64); err != nil {
		return nil, err
	}

	values := make([]float64, len(tensor.Data)/8)
	for i := range values {
		bits := binary.LittleEndian.Uint64(tensor.Data[i*8:])
		values[i] = math.Float64frombits(bits)
	}
	return values, nil
}

func DecodeFP32Values(tensor BinaryTensor) ([]float32, error) {
	if err := requireTensorType(tensor, TensorDataTypeFP32); err != nil {
		return nil, err
	}

	values := make([]float32, len(tensor.Data)/4)
	for i := range values {
		bits := binary.LittleEndian.Uint32(
			tensor.Data[i*4:],
		)
		values[i] = math.Float32frombits(bits)
	}
	return values, nil
}

func DecodeINT32Values(tensor BinaryTensor) ([]int32, error) {
	if err := requireTensorType(tensor, TensorDataTypeINT32); err != nil {
		return nil, err
	}

	values := make([]int32, len(tensor.Data)/4)
	for i := range values {
		values[i] = int32(
			binary.LittleEndian.Uint32(tensor.Data[i*4:]),
		)
	}
	return values, nil
}

func DecodeBOOLValues(tensor BinaryTensor) ([]bool, error) {
	if err := requireTensorType(tensor, TensorDataTypeBOOL); err != nil {
		return nil, err
	}

	values := make([]bool, len(tensor.Data))
	for i, value := range tensor.Data {
		values[i] = value == 1
	}
	return values, nil
}
