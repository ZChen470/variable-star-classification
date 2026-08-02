package triton

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"testing"
)

func TestBinaryTensorTypedRoundTrip(t *testing.T) {
	fp64Tensor, err := NewFP64Tensor(
		"FP64_INPUT",
		[]int64{2},
		[]float64{1.25, -2.5},
	)
	if err != nil {
		t.Fatalf("NewFP64Tensor() error = %v", err)
	}

	fp64Values, err := DecodeFP64Values(fp64Tensor)
	if err != nil {
		t.Fatalf("DecodeFP64Values() error = %v", err)
	}
	if !reflect.DeepEqual(
		fp64Values,
		[]float64{1.25, -2.5},
	) {
		t.Fatalf("FP64 values = %v", fp64Values)
	}

	fp32Tensor, err := NewFP32Tensor(
		"FP32_INPUT",
		[]int64{2},
		[]float32{0.25, 0.75},
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor() error = %v", err)
	}

	fp32Values, err := DecodeFP32Values(fp32Tensor)
	if err != nil {
		t.Fatalf("DecodeFP32Values() error = %v", err)
	}
	if !reflect.DeepEqual(
		fp32Values,
		[]float32{0.25, 0.75},
	) {
		t.Fatalf("FP32 values = %v", fp32Values)
	}

	int32Tensor, err := NewINT32Tensor(
		"INT32_INPUT",
		[]int64{2},
		[]int32{-1, 3},
	)
	if err != nil {
		t.Fatalf("NewINT32Tensor() error = %v", err)
	}

	int32Values, err := DecodeINT32Values(int32Tensor)
	if err != nil {
		t.Fatalf("DecodeINT32Values() error = %v", err)
	}
	if !reflect.DeepEqual(
		int32Values,
		[]int32{-1, 3},
	) {
		t.Fatalf("INT32 values = %v", int32Values)
	}

	boolTensor, err := NewBOOLTensor(
		"BOOL_INPUT",
		[]int64{3},
		[]bool{true, false, true},
	)
	if err != nil {
		t.Fatalf("NewBOOLTensor() error = %v", err)
	}

	boolValues, err := DecodeBOOLValues(boolTensor)
	if err != nil {
		t.Fatalf("DecodeBOOLValues() error = %v", err)
	}
	if !reflect.DeepEqual(
		boolValues,
		[]bool{true, false, true},
	) {
		t.Fatalf("BOOL values = %v", boolValues)
	}
}

func TestEncodeBinaryInferRequest(t *testing.T) {
	timeTensor, err := NewFP64Tensor(
		"TIME_MJD",
		[]int64{2},
		[]float64{60000.25, 60001.5},
	)
	if err != nil {
		t.Fatalf("NewFP64Tensor() error = %v", err)
	}

	magnitudeTensor, err := NewFP32Tensor(
		"MAGNITUDE",
		[]int64{2},
		[]float32{14.5, 14.75},
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor() error = %v", err)
	}

	modeTensor, err := NewINT32Tensor(
		"COARSE_MODE",
		[]int64{1},
		[]int32{1},
	)
	if err != nil {
		t.Fatalf("NewINT32Tensor() error = %v", err)
	}

	request, err := EncodeBinaryInferRequest(
		"variable_star_classifier",
		"1",
		"job-123",
		[]BinaryTensor{
			timeTensor,
			magnitudeTensor,
			modeTensor,
		},
		[]string{
			"COARSE_PROBS",
			"XGBOOST_EXECUTED",
		},
	)
	if err != nil {
		t.Fatalf("EncodeBinaryInferRequest() error = %v", err)
	}

	if request.Operation != ModelOperationInfer {
		t.Fatalf(
			"Operation = %d, want infer",
			request.Operation,
		)
	}

	if request.Header.Get("Content-Type") != BinaryContentType {
		t.Fatalf(
			"Content-Type = %q",
			request.Header.Get("Content-Type"),
		)
	}

	headerLength, err := strconv.Atoi(
		request.Header.Get(
			InferenceHeaderContentLength,
		),
	)
	if err != nil {
		t.Fatalf("strconv.Atoi() error = %v", err)
	}

	if headerLength <= 0 || headerLength >= len(request.Body) {
		t.Fatalf(
			"header length = %d, body length = %d",
			headerLength,
			len(request.Body),
		)
	}

	var header binaryInferRequestHeader
	if err := json.Unmarshal(
		request.Body[:headerLength],
		&header,
	); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if header.ID != "job-123" {
		t.Fatalf("header ID = %q", header.ID)
	}

	if len(header.Inputs) != 3 {
		t.Fatalf(
			"input count = %d, want 3",
			len(header.Inputs),
		)
	}

	if header.Inputs[0].Name != "TIME_MJD" ||
		header.Inputs[1].Name != "MAGNITUDE" ||
		header.Inputs[2].Name != "COARSE_MODE" {
		t.Fatalf(
			"input order = %q, %q, %q",
			header.Inputs[0].Name,
			header.Inputs[1].Name,
			header.Inputs[2].Name,
		)
	}

	if !header.Outputs[0].Parameters.BinaryData ||
		!header.Outputs[1].Parameters.BinaryData {
		t.Fatal("outputs did not request binary data")
	}

	wantPayload := make([]byte, 0)
	wantPayload = append(wantPayload, timeTensor.Data...)
	wantPayload = append(wantPayload, magnitudeTensor.Data...)
	wantPayload = append(wantPayload, modeTensor.Data...)

	if !bytes.Equal(
		request.Body[headerLength:],
		wantPayload,
	) {
		t.Fatal("binary payload order does not match inputs")
	}
}

func TestDecodeBinaryInferResponseUsesJSONOutputOrder(t *testing.T) {
	leafTensor, err := NewFP32Tensor(
		"LEAF_PROBS",
		[]int64{2},
		[]float32{0.25, 0.75},
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor() error = %v", err)
	}

	executedTensor, err := NewBOOLTensor(
		"XGBOOST_EXECUTED",
		[]int64{1},
		[]bool{true},
	)
	if err != nil {
		t.Fatalf("NewBOOLTensor() error = %v", err)
	}

	leafSize := int64(len(leafTensor.Data))
	executedSize := int64(len(executedTensor.Data))

	header := binaryInferResponseHeader{
		ID:           "job-123",
		ModelName:    "variable_star_classifier",
		ModelVersion: "1",
		Outputs: []binaryInferResponseOutputHeader{
			{
				Name:     leafTensor.Name,
				Shape:    leafTensor.Shape,
				DataType: leafTensor.DataType,
				Parameters: binaryResponseParameters{
					BinaryDataSize: &leafSize,
				},
			},
			{
				Name:     executedTensor.Name,
				Shape:    executedTensor.Shape,
				DataType: executedTensor.DataType,
				Parameters: binaryResponseParameters{
					BinaryDataSize: &executedSize,
				},
			},
		},
	}

	jsonHeader, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	body := append([]byte(nil), jsonHeader...)
	body = append(body, leafTensor.Data...)
	body = append(body, executedTensor.Data...)

	decoded, err := DecodeBinaryInferResponse(
		ModelResponse{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": []string{
					BinaryContentType,
				},
				InferenceHeaderContentLength: []string{
					strconv.Itoa(len(jsonHeader)),
				},
			},
			Body: body,
		},
	)
	if err != nil {
		t.Fatalf(
			"DecodeBinaryInferResponse() error = %v",
			err,
		)
	}

	if len(decoded.Outputs) != 2 {
		t.Fatalf(
			"output count = %d, want 2",
			len(decoded.Outputs),
		)
	}

	if decoded.Outputs[0].Name != "LEAF_PROBS" ||
		decoded.Outputs[1].Name != "XGBOOST_EXECUTED" {
		t.Fatalf(
			"output order = %q, %q",
			decoded.Outputs[0].Name,
			decoded.Outputs[1].Name,
		)
	}

	leafValues, err := DecodeFP32Values(decoded.Outputs[0])
	if err != nil {
		t.Fatalf("DecodeFP32Values() error = %v", err)
	}
	if !reflect.DeepEqual(
		leafValues,
		[]float32{0.25, 0.75},
	) {
		t.Fatalf("leaf values = %v", leafValues)
	}

	executedValues, err := DecodeBOOLValues(decoded.Outputs[1])
	if err != nil {
		t.Fatalf("DecodeBOOLValues() error = %v", err)
	}
	if !reflect.DeepEqual(
		executedValues,
		[]bool{true},
	) {
		t.Fatalf(
			"executed values = %v",
			executedValues,
		)
	}
}

func TestEncodeBinaryInferRequestRejectsInvalidInput(t *testing.T) {
	validTensor, err := NewFP32Tensor(
		"INPUT",
		[]int64{1},
		[]float32{1},
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor() error = %v", err)
	}

	tests := []struct {
		name    string
		inputs  []BinaryTensor
		outputs []string
	}{
		{
			name: "no inputs",
			outputs: []string{
				"OUTPUT",
			},
		},
		{
			name: "duplicate inputs",
			inputs: []BinaryTensor{
				validTensor,
				validTensor,
			},
			outputs: []string{
				"OUTPUT",
			},
		},
		{
			name: "wrong binary size",
			inputs: []BinaryTensor{
				{
					Name:     "INPUT",
					DataType: TensorDataTypeFP32,
					Shape:    []int64{2},
					Data:     make([]byte, 4),
				},
			},
			outputs: []string{
				"OUTPUT",
			},
		},
		{
			name: "invalid BOOL byte",
			inputs: []BinaryTensor{
				{
					Name:     "INPUT",
					DataType: TensorDataTypeBOOL,
					Shape:    []int64{1},
					Data:     []byte{2},
				},
			},
			outputs: []string{
				"OUTPUT",
			},
		},
		{
			name:   "no outputs",
			inputs: []BinaryTensor{validTensor},
		},
		{
			name:   "duplicate outputs",
			inputs: []BinaryTensor{validTensor},
			outputs: []string{
				"OUTPUT",
				"OUTPUT",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, encodeErr := EncodeBinaryInferRequest(
				"variable_star_classifier",
				"1",
				"request",
				test.inputs,
				test.outputs,
			)

			if !errors.Is(
				encodeErr,
				ErrInvalidBinaryInferRequest,
			) {
				t.Fatalf(
					"error = %v, want ErrInvalidBinaryInferRequest",
					encodeErr,
				)
			}
		})
	}
}

func TestDecodeBinaryInferResponseRejectsInvalidBody(t *testing.T) {
	validTensor, err := NewFP32Tensor(
		"OUTPUT",
		[]int64{1},
		[]float32{1},
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor() error = %v", err)
	}

	size := int64(len(validTensor.Data))
	header := binaryInferResponseHeader{
		ModelName:    "variable_star_classifier",
		ModelVersion: "1",
		Outputs: []binaryInferResponseOutputHeader{
			{
				Name:     validTensor.Name,
				Shape:    validTensor.Shape,
				DataType: validTensor.DataType,
				Parameters: binaryResponseParameters{
					BinaryDataSize: &size,
				},
			},
		},
	}

	jsonHeader, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	validBody := append([]byte(nil), jsonHeader...)
	validBody = append(validBody, validTensor.Data...)

	tests := []struct {
		name    string
		headers http.Header
		body    []byte
		status  int
	}{
		{
			name: "wrong content type",
			headers: http.Header{
				"Content-Type": []string{
					"application/json",
				},
				InferenceHeaderContentLength: []string{
					strconv.Itoa(len(jsonHeader)),
				},
			},
			body:   validBody,
			status: http.StatusOK,
		},
		{
			name: "missing header length",
			headers: http.Header{
				"Content-Type": []string{
					BinaryContentType,
				},
			},
			body:   validBody,
			status: http.StatusOK,
		},
		{
			name: "header exceeds body",
			headers: http.Header{
				"Content-Type": []string{
					BinaryContentType,
				},
				InferenceHeaderContentLength: []string{
					strconv.Itoa(len(validBody) + 1),
				},
			},
			body:   validBody,
			status: http.StatusOK,
		},
		{
			name: "trailing binary bytes",
			headers: http.Header{
				"Content-Type": []string{
					BinaryContentType,
				},
				InferenceHeaderContentLength: []string{
					strconv.Itoa(len(jsonHeader)),
				},
			},
			body: append(
				append([]byte(nil), validBody...),
				99,
			),
			status: http.StatusOK,
		},
		{
			name: "non-success status",
			headers: http.Header{
				"Content-Type": []string{
					BinaryContentType,
				},
				InferenceHeaderContentLength: []string{
					strconv.Itoa(len(jsonHeader)),
				},
			},
			body:   validBody,
			status: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, decodeErr := DecodeBinaryInferResponse(
				ModelResponse{
					StatusCode: test.status,
					Header:     test.headers,
					Body:       test.body,
				},
			)

			if !errors.Is(
				decodeErr,
				ErrInvalidBinaryInferResponse,
			) {
				t.Fatalf(
					"error = %v, want ErrInvalidBinaryInferResponse",
					decodeErr,
				)
			}
		})
	}
}

func TestFP32EncodingUsesLittleEndian(t *testing.T) {
	tensor, err := NewFP32Tensor(
		"INPUT",
		[]int64{1},
		[]float32{1},
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor() error = %v", err)
	}

	want := []byte{0x00, 0x00, 0x80, 0x3f}

	if !bytes.Equal(tensor.Data, want) {
		t.Fatalf(
			"encoded bytes = %v, want %v",
			tensor.Data,
			want,
		)
	}

	if math.Float32bits(1) != 0x3f800000 {
		t.Fatal("unexpected float32 representation")
	}
}
