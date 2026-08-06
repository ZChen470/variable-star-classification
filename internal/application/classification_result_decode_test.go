package application_test

import (
	"errors"
	"reflect"
	"testing"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
)

const classificationResultDecodeTopic = "astro.classification.results.v1"

func TestDecodeClassificationResultMessageMapsValidResult(
	t *testing.T,
) {
	run := validClassificationResultDecodeRun(t)

	traceContext := application.TraceContext{
		TraceID:       "trace-001",
		CorrelationID: "correlation-001",
		CausationID:   "command-001",
	}

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		traceContext,
	)

	got, err := application.DecodeClassificationResultMessage(
		classificationResultDecodeTopic,
		message,
	)
	if err != nil {
		t.Fatalf(
			"DecodeClassificationResultMessage() error = %v",
			err,
		)
	}

	wantRun := run
	wantRun.CompletedAt = run.CompletedAt.UTC()

	if !reflect.DeepEqual(got.Run, wantRun) {
		t.Fatalf(
			"Run mismatch:\ngot  = %#v\nwant = %#v",
			got.Run,
			wantRun,
		)
	}

	if !reflect.DeepEqual(
		got.TraceContext,
		traceContext,
	) {
		t.Fatalf(
			"TraceContext = %#v, want %#v",
			got.TraceContext,
			traceContext,
		)
	}
}

func TestDecodeClassificationResultMessageMapsCoarseSourceVariants(
	t *testing.T,
) {
	sourceRunID := domain.RunID(
		"22222222-2222-2222-2222-222222222222",
	)

	tests := []struct {
		name   string
		mutate func(*domain.ClassificationRun)
	}{
		{
			name: "computed current",
			mutate: func(
				run *domain.ClassificationRun,
			) {
				run.EffectiveEpochCount = 3
				run.CoarseSourceType =
					domain.CoarseSourceComputedCurrent
				run.CoarseSourceRunID = nil
				run.CoarseSourceLightCurveRevision =
					run.LightCurveRevision
				run.CoarseSourceEpochCount = 3
				run.XGBoostExecuted = true
			},
		},
		{
			name: "reused previous",
			mutate: func(
				run *domain.ClassificationRun,
			) {
				run.EffectiveEpochCount = 21
				run.CoarseSourceType =
					domain.CoarseSourceReusedPrevious
				run.CoarseSourceRunID = &sourceRunID
				run.CoarseSourceLightCurveRevision = 9
				run.CoarseSourceEpochCount = 20
				run.XGBoostExecuted = false
			},
		},
		{
			name: "computed bootstrap",
			mutate: func(
				run *domain.ClassificationRun,
			) {
				run.EffectiveEpochCount = 21
				run.CoarseSourceType =
					domain.CoarseSourceComputedBootstrap
				run.CoarseSourceRunID = nil
				run.CoarseSourceLightCurveRevision =
					run.LightCurveRevision
				run.CoarseSourceEpochCount = 21
				run.XGBoostExecuted = true
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := validClassificationResultDecodeRun(t)
			test.mutate(&run)

			message :=
				buildClassificationResultDecodeMessage(
					t,
					run,
					application.TraceContext{},
				)

			got, err :=
				application.DecodeClassificationResultMessage(
					classificationResultDecodeTopic,
					message,
				)
			if err != nil {
				t.Fatalf(
					"DecodeClassificationResultMessage() error = %v",
					err,
				)
			}

			want := run
			want.CompletedAt = run.CompletedAt.UTC()

			if !reflect.DeepEqual(got.Run, want) {
				t.Fatalf(
					"Run mismatch:\ngot  = %#v\nwant = %#v",
					got.Run,
					want,
				)
			}
		})
	}
}

func TestDecodeClassificationResultMessageRejectsPermanentErrors(
	t *testing.T,
) {
	baseRun := validClassificationResultDecodeRun(t)

	baseMessage :=
		buildClassificationResultDecodeMessage(
			t,
			baseRun,
			application.TraceContext{},
		)

	tests := []struct {
		name string

		mutate func(
			*testing.T,
			*application.InboundMessage,
		)

		wantCode  application.ClassificationResultErrorCode
		wantField string
	}{
		{
			name: "unexpected topic",
			mutate: func(
				_ *testing.T,
				message *application.InboundMessage,
			) {
				message.Topic = "unexpected.topic"
			},
			wantCode: application.
				ClassificationResultErrorCodeUnexpectedTopic,
			wantField: "topic",
		},
		{
			name: "empty key",
			mutate: func(
				_ *testing.T,
				message *application.InboundMessage,
			) {
				message.Key = nil
			},
			wantCode: application.
				ClassificationResultErrorCodeInvalidField,
			wantField: "key",
		},
		{
			name: "empty value",
			mutate: func(
				_ *testing.T,
				message *application.InboundMessage,
			) {
				message.Value = nil
			},
			wantCode: application.
				ClassificationResultErrorCodeMalformedMessage,
			wantField: "value",
		},
		{
			name: "malformed protobuf",
			mutate: func(
				_ *testing.T,
				message *application.InboundMessage,
			) {
				message.Value = []byte{0xff}
			},
			wantCode: application.
				ClassificationResultErrorCodeMalformedMessage,
			wantField: "value",
		},
		{
			name: "Kafka key mismatch",
			mutate: func(
				_ *testing.T,
				message *application.InboundMessage,
			) {
				message.Key = []byte("OTHER-OBJECT")
			},
			wantCode: application.
				ClassificationResultErrorCodeKeyMismatch,
			wantField: "key",
		},
		{
			name: "job ID mismatch",
			mutate: func(
				t *testing.T,
				message *application.InboundMessage,
			) {
				mutateClassificationResultProto(
					t,
					message,
					func(
						result *classificationv1.
							ClassificationResult,
					) {
						result.JobId =
							"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
					},
				)
			},
			wantCode: application.
				ClassificationResultErrorCodeJobIDMismatch,
			wantField: "job_id",
		},
		{
			name: "run ID mismatch",
			mutate: func(
				t *testing.T,
				message *application.InboundMessage,
			) {
				mutateClassificationResultProto(
					t,
					message,
					func(
						result *classificationv1.
							ClassificationResult,
					) {
						result.RunId =
							"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
					},
				)
			},
			wantCode: application.
				ClassificationResultErrorCodeRunIDMismatch,
			wantField: "run_id",
		},
		{
			name: "missing versions",
			mutate: func(
				t *testing.T,
				message *application.InboundMessage,
			) {
				mutateClassificationResultProto(
					t,
					message,
					func(
						result *classificationv1.
							ClassificationResult,
					) {
						result.Versions = nil
					},
				)
			},
			wantCode: application.
				ClassificationResultErrorCodeInvalidField,
			wantField: "versions",
		},
		{
			name: "missing leaf probabilities",
			mutate: func(
				t *testing.T,
				message *application.InboundMessage,
			) {
				mutateClassificationResultProto(
					t,
					message,
					func(
						result *classificationv1.
							ClassificationResult,
					) {
						result.LeafProbabilities = nil
					},
				)
			},
			wantCode: application.
				ClassificationResultErrorCodeInvalidField,
			wantField: "leaf_probabilities",
		},
		{
			name: "predicted coarse class mismatch",
			mutate: func(
				t *testing.T,
				message *application.InboundMessage,
			) {
				mutateClassificationResultProto(
					t,
					message,
					func(
						result *classificationv1.
							ClassificationResult,
					) {
						result.PredictedCoarseClass =
							classificationv1.
								CoarseClass_COARSE_CLASS_ROTATING
					},
				)
			},
			wantCode: application.
				ClassificationResultErrorCodeInvalidField,
			wantField: "predicted_coarse_class",
		},
		{
			name: "invalid coarse source relationship",
			mutate: func(
				t *testing.T,
				message *application.InboundMessage,
			) {
				mutateClassificationResultProto(
					t,
					message,
					func(
						result *classificationv1.
							ClassificationResult,
					) {
						result.XgboostExecuted = false
					},
				)
			},
			wantCode: application.
				ClassificationResultErrorCodeInvalidField,
			wantField: "coarse_source",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := baseMessage
			message.Key = append(
				[]byte(nil),
				baseMessage.Key...,
			)
			message.Value = append(
				[]byte(nil),
				baseMessage.Value...,
			)

			test.mutate(t, &message)

			got, err :=
				application.DecodeClassificationResultMessage(
					classificationResultDecodeTopic,
					message,
				)
			if err == nil {
				t.Fatalf(
					"DecodeClassificationResultMessage() error = nil, got %#v",
					got,
				)
			}

			var permanentError *application.
				PermanentClassificationResultError

			if !errors.As(err, &permanentError) {
				t.Fatalf(
					"error = %v, want PermanentClassificationResultError",
					err,
				)
			}

			if permanentError.Code != test.wantCode {
				t.Fatalf(
					"Code = %q, want %q",
					permanentError.Code,
					test.wantCode,
				)
			}

			if permanentError.Field != test.wantField {
				t.Fatalf(
					"Field = %q, want %q",
					permanentError.Field,
					test.wantField,
				)
			}
		})
	}
}

func TestDecodeClassificationResultMessageDoesNotValidateProbabilityRange(
	t *testing.T,
) {
	run := validClassificationResultDecodeRun(t)

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		application.TraceContext{},
	)

	mutateClassificationResultProto(
		t,
		&message,
		func(result *classificationv1.ClassificationResult) {
			result.CoarseProbabilities.Rotating = -1
			result.CoarseProbabilities.Cataclysmic = 2

			result.PredictedCoarseClass =
				classificationv1.
					CoarseClass_COARSE_CLASS_CATACLYSMIC

			result.FineConditionalProbabilities.
				EwGivenEclipsingBinary = -1

			result.FineConditionalProbabilities.
				EaGivenEclipsingBinary = 2

			result.LeafProbabilities.Ew = -1
			result.LeafProbabilities.Ea = 2

			result.PredictedLeafClass =
				classificationv1.
					LeafClass_LEAF_CLASS_EA
		},
	)

	got, err := application.DecodeClassificationResultMessage(
		classificationResultDecodeTopic,
		message,
	)
	if err != nil {
		t.Fatalf(
			"DecodeClassificationResultMessage() error = %v",
			err,
		)
	}

	if got.Run.CoarseProbabilities[0] != -1 ||
		got.Run.CoarseProbabilities[1] != 2 {
		t.Fatalf(
			"coarse probabilities = %#v",
			got.Run.CoarseProbabilities,
		)
	}

	if got.Run.PredictedCoarseClass !=
		domain.CoarseClassCataclysmic {
		t.Fatalf(
			"PredictedCoarseClass = %d, want %d",
			got.Run.PredictedCoarseClass,
			domain.CoarseClassCataclysmic,
		)
	}

	if got.Run.LeafProbabilities[0] != -1 ||
		got.Run.LeafProbabilities[1] != 2 {
		t.Fatalf(
			"leaf probabilities = %#v",
			got.Run.LeafProbabilities,
		)
	}

	if got.Run.PredictedLeafClass !=
		domain.LeafClassEA {
		t.Fatalf(
			"PredictedLeafClass = %d, want %d",
			got.Run.PredictedLeafClass,
			domain.LeafClassEA,
		)
	}
}

func validClassificationResultDecodeRun(
	t *testing.T,
) domain.ClassificationRun {
	t.Helper()

	run := validClassificationResultRun(t)

	jobID, err := domain.GenerateJobID(
		domain.JobIdentity{
			ObjectID:           run.ObjectID,
			LightCurveRevision: run.LightCurveRevision,
			ModelBundleVersion: run.Versions.ModelBundleVersion,
			ExecutionMode:      run.ExecutionMode,
		},
	)
	if err != nil {
		t.Fatalf(
			"GenerateJobID() error = %v",
			err,
		)
	}

	runID, err := domain.GenerateRunID(jobID)
	if err != nil {
		t.Fatalf(
			"GenerateRunID() error = %v",
			err,
		)
	}

	run.JobID = jobID
	run.RunID = runID

	return run
}

func buildClassificationResultDecodeMessage(
	t *testing.T,
	run domain.ClassificationRun,
	traceContext application.TraceContext,
) application.InboundMessage {
	t.Helper()

	outbound, err :=
		application.BuildClassificationResultMessage(
			classificationResultDecodeTopic,
			run,
			traceContext,
			nil,
		)
	if err != nil {
		t.Fatalf(
			"BuildClassificationResultMessage() error = %v",
			err,
		)
	}

	return application.InboundMessage{
		Topic:   outbound.Topic,
		Key:     append([]byte(nil), outbound.Key...),
		Value:   append([]byte(nil), outbound.Value...),
		Headers: outbound.Headers,

		// Decoder 不依赖 Kafka Record Timestamp。
	}
}

func mutateClassificationResultProto(
	t *testing.T,
	message *application.InboundMessage,
	mutate func(*classificationv1.ClassificationResult),
) {
	t.Helper()

	var result classificationv1.ClassificationResult

	if err := proto.Unmarshal(
		message.Value,
		&result,
	); err != nil {
		t.Fatalf(
			"proto.Unmarshal() error = %v",
			err,
		)
	}

	mutate(&result)

	value, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(&result)
	if err != nil {
		t.Fatalf(
			"proto.Marshal() error = %v",
			err,
		)
	}

	message.Value = value
}
