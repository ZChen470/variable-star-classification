package application_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBuildClassificationResultMessageMapsRun(t *testing.T) {
	run := validClassificationResultRun(t)

	traceContext := application.TraceContext{
		TraceID:       "trace-001",
		CorrelationID: "correlation-001",
		CausationID:   "command-001",
	}

	message, err := application.BuildClassificationResultMessage(
		"astro.classification.results.v1",
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

	if message.Topic != "astro.classification.results.v1" {
		t.Fatalf(
			"Topic = %q, want %q",
			message.Topic,
			"astro.classification.results.v1",
		)
	}

	if !bytes.Equal(message.Key, []byte(run.ObjectID)) {
		t.Fatalf(
			"Key = %q, want %q",
			message.Key,
			run.ObjectID,
		)
	}

	if !message.Timestamp.Equal(run.CompletedAt) {
		t.Fatalf(
			"Timestamp = %v, want %v",
			message.Timestamp,
			run.CompletedAt,
		)
	}

	if message.Headers != nil {
		t.Fatalf(
			"Headers = %#v, want nil",
			message.Headers,
		)
	}

	var got classificationv1.ClassificationResult
	if err := proto.Unmarshal(message.Value, &got); err != nil {
		t.Fatalf(
			"proto.Unmarshal() error = %v",
			err,
		)
	}

	want := expectedClassificationResult(run, traceContext)

	if !proto.Equal(&got, want) {
		t.Fatalf(
			"ClassificationResult mismatch:\ngot  = %v\nwant = %v",
			&got,
			want,
		)
	}
}

func TestBuildClassificationResultMessageMapsCoarseSourceVariants(
	t *testing.T,
) {
	sourceRunID := domain.RunID(
		"22222222-2222-2222-2222-222222222222",
	)

	tests := []struct {
		name string

		mutate func(*domain.ClassificationRun)

		wantSourceType      classificationv1.CoarseSourceType
		wantSourceRunID     *string
		wantXGBoostExecuted bool
	}{
		{
			name: "computed current",
			mutate: func(run *domain.ClassificationRun) {
				run.CoarseSourceType =
					domain.CoarseSourceComputedCurrent
				run.CoarseSourceRunID = nil
				run.CoarseSourceLightCurveRevision =
					run.LightCurveRevision
				run.CoarseSourceEpochCount =
					run.EffectiveEpochCount
				run.XGBoostExecuted = true
			},
			wantSourceType: classificationv1.
				CoarseSourceType_COARSE_SOURCE_COMPUTED_CURRENT,
			wantSourceRunID:     nil,
			wantXGBoostExecuted: true,
		},
		{
			name: "reused previous",
			mutate: func(run *domain.ClassificationRun) {
				run.EffectiveEpochCount = 21
				run.CoarseSourceType =
					domain.CoarseSourceReusedPrevious
				run.CoarseSourceRunID = &sourceRunID
				run.CoarseSourceLightCurveRevision = 9
				run.CoarseSourceEpochCount = 20
				run.XGBoostExecuted = false
			},
			wantSourceType: classificationv1.
				CoarseSourceType_COARSE_SOURCE_REUSED_PREVIOUS,
			wantSourceRunID: stringPointer(
				string(sourceRunID),
			),
			wantXGBoostExecuted: false,
		},
		{
			name: "computed bootstrap",
			mutate: func(run *domain.ClassificationRun) {
				run.EffectiveEpochCount = 21
				run.CoarseSourceType =
					domain.CoarseSourceComputedBootstrap
				run.CoarseSourceRunID = nil
				run.CoarseSourceLightCurveRevision =
					run.LightCurveRevision
				run.CoarseSourceEpochCount = 21
				run.XGBoostExecuted = true
			},
			wantSourceType: classificationv1.
				CoarseSourceType_COARSE_SOURCE_COMPUTED_BOOTSTRAP,
			wantSourceRunID:     nil,
			wantXGBoostExecuted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := validClassificationResultRun(t)
			test.mutate(&run)

			message, err :=
				application.BuildClassificationResultMessage(
					"astro.classification.results.v1",
					run,
					application.TraceContext{},
					nil,
				)
			if err != nil {
				t.Fatalf(
					"BuildClassificationResultMessage() error = %v",
					err,
				)
			}

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

			if result.GetCoarseSourceType() !=
				test.wantSourceType {
				t.Fatalf(
					"CoarseSourceType = %v, want %v",
					result.GetCoarseSourceType(),
					test.wantSourceType,
				)
			}

			if !reflect.DeepEqual(
				result.CoarseSourceRunId,
				test.wantSourceRunID,
			) {
				t.Fatalf(
					"CoarseSourceRunId = %#v, want %#v",
					result.CoarseSourceRunId,
					test.wantSourceRunID,
				)
			}

			if result.GetXgboostExecuted() !=
				test.wantXGBoostExecuted {
				t.Fatalf(
					"XgboostExecuted = %v, want %v",
					result.GetXgboostExecuted(),
					test.wantXGBoostExecuted,
				)
			}
		})
	}
}

func TestBuildClassificationResultMessageCopiesHeaders(t *testing.T) {
	run := validClassificationResultRun(t)

	value := []byte("original")

	headers := []application.MessageHeader{
		{
			Key:   "x-nil",
			Value: nil,
		},
		{
			Key:   "x-empty",
			Value: make([]byte, 0),
		},
		{
			Key:   "x-value",
			Value: value,
		},
	}

	message, err := application.BuildClassificationResultMessage(
		"astro.classification.results.v1",
		run,
		application.TraceContext{},
		headers,
	)
	if err != nil {
		t.Fatalf(
			"BuildClassificationResultMessage() error = %v",
			err,
		)
	}

	if len(message.Headers) != len(headers) {
		t.Fatalf(
			"Headers length = %d, want %d",
			len(message.Headers),
			len(headers),
		)
	}

	if message.Headers[0].Value != nil {
		t.Fatalf(
			"nil Header value became %#v",
			message.Headers[0].Value,
		)
	}

	if message.Headers[1].Value == nil {
		t.Fatal(
			"non-nil empty Header value became nil",
		)
	}

	if len(message.Headers[1].Value) != 0 {
		t.Fatalf(
			"empty Header length = %d, want 0",
			len(message.Headers[1].Value),
		)
	}

	if string(message.Headers[2].Value) != "original" {
		t.Fatalf(
			"Header value = %q, want %q",
			message.Headers[2].Value,
			"original",
		)
	}

	headers[0].Key = "changed"
	value[0] = 'X'

	if message.Headers[0].Key != "x-nil" {
		t.Fatalf(
			"Header Key changed through input alias: %q",
			message.Headers[0].Key,
		)
	}

	if string(message.Headers[2].Value) != "original" {
		t.Fatalf(
			"Header Value changed through input alias: %q",
			message.Headers[2].Value,
		)
	}

	message.Headers[2].Value[1] = 'Y'

	if string(value) != "Xriginal" {
		t.Fatalf(
			"input Header Value changed through output alias: %q",
			value,
		)
	}
}

func TestBuildClassificationResultMessagePreservesHeaderSliceShape(
	t *testing.T,
) {
	run := validClassificationResultRun(t)

	tests := []struct {
		name    string
		headers []application.MessageHeader
		wantNil bool
	}{
		{
			name:    "nil headers",
			headers: nil,
			wantNil: true,
		},
		{
			name:    "non-nil empty headers",
			headers: make([]application.MessageHeader, 0),
			wantNil: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err :=
				application.BuildClassificationResultMessage(
					"astro.classification.results.v1",
					run,
					application.TraceContext{},
					test.headers,
				)
			if err != nil {
				t.Fatalf(
					"BuildClassificationResultMessage() error = %v",
					err,
				)
			}

			if (message.Headers == nil) != test.wantNil {
				t.Fatalf(
					"Headers nil = %v, want %v",
					message.Headers == nil,
					test.wantNil,
				)
			}

			if len(message.Headers) != 0 {
				t.Fatalf(
					"Headers length = %d, want 0",
					len(message.Headers),
				)
			}
		})
	}
}

func TestBuildClassificationResultMessageOmitsEmptyTraceContext(
	t *testing.T,
) {
	run := validClassificationResultRun(t)

	message, err := application.BuildClassificationResultMessage(
		"astro.classification.results.v1",
		run,
		application.TraceContext{},
		nil,
	)
	if err != nil {
		t.Fatalf(
			"BuildClassificationResultMessage() error = %v",
			err,
		)
	}

	var result classificationv1.ClassificationResult
	if err := proto.Unmarshal(message.Value, &result); err != nil {
		t.Fatalf(
			"proto.Unmarshal() error = %v",
			err,
		)
	}

	if result.TraceContext != nil {
		t.Fatalf(
			"TraceContext = %#v, want nil",
			result.TraceContext,
		)
	}
}

func TestBuildClassificationResultMessageIsDeterministic(t *testing.T) {
	run := validClassificationResultRun(t)

	sourceRunID := domain.RunID(
		"22222222-2222-2222-2222-222222222222",
	)

	run.EffectiveEpochCount = 21
	run.CoarseSourceType = domain.CoarseSourceReusedPrevious
	run.CoarseSourceRunID = &sourceRunID
	run.CoarseSourceLightCurveRevision = 9
	run.CoarseSourceEpochCount = 20
	run.XGBoostExecuted = false

	traceContext := application.TraceContext{
		TraceID:       "trace-001",
		CorrelationID: "correlation-001",
		CausationID:   "command-001",
	}

	headers := []application.MessageHeader{
		{
			Key:   "x-test",
			Value: []byte("value"),
		},
	}

	first, err := application.BuildClassificationResultMessage(
		"astro.classification.results.v1",
		run,
		traceContext,
		headers,
	)
	if err != nil {
		t.Fatalf(
			"first BuildClassificationResultMessage() error = %v",
			err,
		)
	}

	second, err := application.BuildClassificationResultMessage(
		"astro.classification.results.v1",
		run,
		traceContext,
		headers,
	)
	if err != nil {
		t.Fatalf(
			"second BuildClassificationResultMessage() error = %v",
			err,
		)
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatalf(
			"repeated build differs:\nfirst  = %#v\nsecond = %#v",
			first,
			second,
		)
	}

	if !bytes.Equal(first.Value, second.Value) {
		t.Fatal(
			"deterministic Protobuf bytes differ",
		)
	}
}

func TestBuildClassificationResultMessageDoesNotRepeatDomainValidation(
	t *testing.T,
) {
	run := validClassificationResultRun(t)

	run.RunID = "not-a-uuid"
	run.JobID = ""
	run.ObjectID = ""
	run.CandidateRevision = -1
	run.LightCurveRevision = -2
	run.EffectiveEpochCount = 0
	run.ExecutionMode = domain.ExecutionMode(99)

	run.CoarseSourceType = domain.CoarseSourceType(99)
	run.CoarseSourceRunID = nil
	run.CoarseSourceLightCurveRevision = -3
	run.CoarseSourceEpochCount = 0
	run.XGBoostExecuted = false

	run.Versions.ModelBundleVersion = ""

	run.CoarseProbabilities =
		[domain.CoarseProbabilityCount]float32{
			-1,
			2,
			0,
			0,
			0,
			0,
			0,
		}

	run.FineConditionalProbabilities =
		[domain.ConditionalFineProbabilityCount]float32{
			-1,
			2,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
		}

	run.LeafProbabilities =
		[domain.LeafProbabilityCount]float32{
			-1,
			2,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
		}

	run.PredictedCoarseClass = domain.CoarseClass(999)
	run.PredictedLeafClass = domain.LeafClass(9999)
	run.CompletedAt = time.Time{}

	message, err := application.BuildClassificationResultMessage(
		"astro.classification.results.v1",
		run,
		application.TraceContext{},
		nil,
	)
	if err != nil {
		t.Fatalf(
			"BuildClassificationResultMessage() repeated domain validation: %v",
			err,
		)
	}

	var result classificationv1.ClassificationResult
	if err := proto.Unmarshal(message.Value, &result); err != nil {
		t.Fatalf(
			"proto.Unmarshal() error = %v",
			err,
		)
	}

	if result.GetRunId() != "not-a-uuid" {
		t.Fatalf(
			"RunId = %q, want %q",
			result.GetRunId(),
			"not-a-uuid",
		)
	}

	if result.GetCandidateRevision() != -1 {
		t.Fatalf(
			"CandidateRevision = %d, want -1",
			result.GetCandidateRevision(),
		)
	}

	if result.GetCoarseSourceType() !=
		classificationv1.CoarseSourceType(99) {
		t.Fatalf(
			"CoarseSourceType = %d, want 99",
			result.GetCoarseSourceType(),
		)
	}

	if result.GetCoarseProbabilities().GetRotating() != -1 {
		t.Fatalf(
			"Rotating probability = %v, want -1",
			result.GetCoarseProbabilities().GetRotating(),
		)
	}

	if result.GetCoarseProbabilities().GetCataclysmic() != 2 {
		t.Fatalf(
			"Cataclysmic probability = %v, want 2",
			result.GetCoarseProbabilities().GetCataclysmic(),
		)
	}
}

func TestClassificationResultDomainEnumsMatchProtoEnums(t *testing.T) {
	tests := []struct {
		name        string
		domainValue int32
		protoValue  int32
	}{
		{
			name:        "execution production",
			domainValue: int32(domain.ExecutionModeProduction),
			protoValue: int32(
				classificationv1.
					ExecutionMode_EXECUTION_MODE_PRODUCTION,
			),
		},
		{
			name:        "execution shadow",
			domainValue: int32(domain.ExecutionModeShadow),
			protoValue: int32(
				classificationv1.
					ExecutionMode_EXECUTION_MODE_SHADOW,
			),
		},
		{
			name:        "execution reprocess",
			domainValue: int32(domain.ExecutionModeReprocess),
			protoValue: int32(
				classificationv1.
					ExecutionMode_EXECUTION_MODE_REPROCESS,
			),
		},
		{
			name: "coarse source computed current",
			domainValue: int32(
				domain.CoarseSourceComputedCurrent,
			),
			protoValue: int32(
				classificationv1.
					CoarseSourceType_COARSE_SOURCE_COMPUTED_CURRENT,
			),
		},
		{
			name: "coarse source reused previous",
			domainValue: int32(
				domain.CoarseSourceReusedPrevious,
			),
			protoValue: int32(
				classificationv1.
					CoarseSourceType_COARSE_SOURCE_REUSED_PREVIOUS,
			),
		},
		{
			name: "coarse source computed bootstrap",
			domainValue: int32(
				domain.CoarseSourceComputedBootstrap,
			),
			protoValue: int32(
				classificationv1.
					CoarseSourceType_COARSE_SOURCE_COMPUTED_BOOTSTRAP,
			),
		},
		{
			name:        "coarse pulsating",
			domainValue: int32(domain.CoarseClassPulsating),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_PULSATING,
			),
		},
		{
			name:        "coarse long period",
			domainValue: int32(domain.CoarseClassLongPeriod),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_LONG_PERIOD,
			),
		},
		{
			name:        "coarse cataclysmic",
			domainValue: int32(domain.CoarseClassCataclysmic),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_CATACLYSMIC,
			),
		},
		{
			name:        "coarse RR Lyrae",
			domainValue: int32(domain.CoarseClassRRLyrae),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_RR_LYRAE,
			),
		},
		{
			name:        "coarse rotating",
			domainValue: int32(domain.CoarseClassRotating),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_ROTATING,
			),
		},
		{
			name: "coarse eclipsing binary",
			domainValue: int32(
				domain.CoarseClassEclipsingBinary,
			),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_ECLIPSING_BINARY,
			),
		},
		{
			name:        "coarse supernova",
			domainValue: int32(domain.CoarseClassSupernova),
			protoValue: int32(
				classificationv1.
					CoarseClass_COARSE_CLASS_SUPERNOVA,
			),
		},
		{
			name:        "leaf cataclysmic",
			domainValue: int32(domain.LeafClassCataclysmic),
			protoValue: int32(
				classificationv1.
					LeafClass_LEAF_CLASS_CATACLYSMIC,
			),
		},
		{
			name:        "leaf supernova",
			domainValue: int32(domain.LeafClassSupernova),
			protoValue: int32(
				classificationv1.
					LeafClass_LEAF_CLASS_SUPERNOVA,
			),
		},
		{
			name:        "leaf DSCT",
			domainValue: int32(domain.LeafClassDSCT),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_DSCT,
			),
		},
		{
			name:        "leaf CEP",
			domainValue: int32(domain.LeafClassCEP),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_CEP,
			),
		},
		{
			name:        "leaf SR",
			domainValue: int32(domain.LeafClassSR),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_SR,
			),
		},
		{
			name:        "leaf Mira",
			domainValue: int32(domain.LeafClassMira),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_MIRA,
			),
		},
		{
			name:        "leaf RRAB",
			domainValue: int32(domain.LeafClassRRAB),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_RRAB,
			),
		},
		{
			name:        "leaf RRC",
			domainValue: int32(domain.LeafClassRRC),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_RRC,
			),
		},
		{
			name:        "leaf BY Dra",
			domainValue: int32(domain.LeafClassByDra),
			protoValue: int32(
				classificationv1.
					LeafClass_LEAF_CLASS_BY_DRA,
			),
		},
		{
			name:        "leaf RS CVn",
			domainValue: int32(domain.LeafClassRSCvn),
			protoValue: int32(
				classificationv1.
					LeafClass_LEAF_CLASS_RS_CVN,
			),
		},
		{
			name:        "leaf EW",
			domainValue: int32(domain.LeafClassEW),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_EW,
			),
		},
		{
			name:        "leaf EA",
			domainValue: int32(domain.LeafClassEA),
			protoValue: int32(
				classificationv1.LeafClass_LEAF_CLASS_EA,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.domainValue != test.protoValue {
				t.Fatalf(
					"domain enum = %d, proto enum = %d",
					test.domainValue,
					test.protoValue,
				)
			}
		})
	}
}

func expectedClassificationResult(
	run domain.ClassificationRun,
	traceContext application.TraceContext,
) *classificationv1.ClassificationResult {
	result := &classificationv1.ClassificationResult{
		RunId:              string(run.RunID),
		JobId:              string(run.JobID),
		ObjectId:           run.ObjectID,
		CandidateRevision:  run.CandidateRevision,
		LightCurveRevision: run.LightCurveRevision,

		EffectiveEpochCount: run.EffectiveEpochCount,

		CoarseSourceType: classificationv1.CoarseSourceType(
			run.CoarseSourceType,
		),

		CoarseSourceLightCurveRevision: run.
			CoarseSourceLightCurveRevision,
		CoarseSourceEpochCount: run.CoarseSourceEpochCount,
		XgboostExecuted:        run.XGBoostExecuted,

		Versions: &classificationv1.ResolvedModelVersions{
			ModelBundleVersion: run.Versions.
				ModelBundleVersion,
		},

		CoarseProbabilities: &classificationv1.
			CoarseProbabilities{
			Rotating:        run.CoarseProbabilities[0],
			Cataclysmic:     run.CoarseProbabilities[1],
			EclipsingBinary: run.CoarseProbabilities[2],
			LongPeriod:      run.CoarseProbabilities[3],
			Pulsating:       run.CoarseProbabilities[4],
			RrLyrae:         run.CoarseProbabilities[5],
			Supernova:       run.CoarseProbabilities[6],
		},

		FineConditionalProbabilities: &classificationv1.FineConditionalProbabilities{
			EwGivenEclipsingBinary: run.FineConditionalProbabilities[0],
			EaGivenEclipsingBinary: run.FineConditionalProbabilities[1],
			ByDraGivenRotating:     run.FineConditionalProbabilities[2],
			RsCvnGivenRotating:     run.FineConditionalProbabilities[3],
			RrabGivenRrLyrae:       run.FineConditionalProbabilities[4],
			RrcGivenRrLyrae:        run.FineConditionalProbabilities[5],
			SrGivenLongPeriod:      run.FineConditionalProbabilities[6],
			MiraGivenLongPeriod:    run.FineConditionalProbabilities[7],
			DsctGivenPulsating:     run.FineConditionalProbabilities[8],
			CepGivenPulsating:      run.FineConditionalProbabilities[9],
		},

		LeafProbabilities: &classificationv1.LeafProbabilities{
			Ew:          run.LeafProbabilities[0],
			Ea:          run.LeafProbabilities[1],
			ByDra:       run.LeafProbabilities[2],
			RsCvn:       run.LeafProbabilities[3],
			Rrab:        run.LeafProbabilities[4],
			Rrc:         run.LeafProbabilities[5],
			Sr:          run.LeafProbabilities[6],
			Mira:        run.LeafProbabilities[7],
			Dsct:        run.LeafProbabilities[8],
			Cep:         run.LeafProbabilities[9],
			Cataclysmic: run.LeafProbabilities[10],
			Supernova:   run.LeafProbabilities[11],
		},

		PredictedCoarseClass: classificationv1.CoarseClass(
			run.PredictedCoarseClass,
		),

		PredictedLeafClass: classificationv1.LeafClass(
			run.PredictedLeafClass,
		),

		CompletedAt: timestamppb.New(run.CompletedAt),

		ExecutionMode: classificationv1.ExecutionMode(
			run.ExecutionMode,
		),
	}

	if run.CoarseSourceRunID != nil {
		sourceRunID := string(*run.CoarseSourceRunID)
		result.CoarseSourceRunId = &sourceRunID
	}

	if traceContext != (application.TraceContext{}) {
		result.TraceContext = &classificationv1.TraceContext{
			TraceId:       traceContext.TraceID,
			CorrelationId: traceContext.CorrelationID,
			CausationId:   traceContext.CausationID,
		}
	}

	return result
}

func validClassificationResultRun(
	t *testing.T,
) domain.ClassificationRun {
	t.Helper()

	jobID := domain.JobID(
		"11111111-1111-1111-1111-111111111111",
	)

	runID, err := domain.GenerateRunID(jobID)
	if err != nil {
		t.Fatalf(
			"GenerateRunID() error = %v",
			err,
		)
	}

	return domain.ClassificationRun{
		RunID:    runID,
		JobID:    jobID,
		ObjectID: "OBJ-0001",

		CandidateRevision:   7,
		LightCurveRevision:  10,
		EffectiveEpochCount: 3,
		ExecutionMode:       domain.ExecutionModeProduction,

		CoarseSourceType:               domain.CoarseSourceComputedCurrent,
		CoarseSourceRunID:              nil,
		CoarseSourceLightCurveRevision: 10,
		CoarseSourceEpochCount:         3,
		XGBoostExecuted:                true,

		Versions: domain.ResolvedModelVersions{
			ModelBundleVersion: "bundle-v1",
		},

		CoarseProbabilities: [domain.CoarseProbabilityCount]float32{
			0.05,
			0.10,
			0.20,
			0.15,
			0.30,
			0.10,
			0.10,
		},

		FineConditionalProbabilities: [domain.ConditionalFineProbabilityCount]float32{
			0.60,
			0.40,
			0.70,
			0.30,
			0.80,
			0.20,
			0.55,
			0.45,
			0.65,
			0.35,
		},

		LeafProbabilities: [domain.LeafProbabilityCount]float32{
			0.12,
			0.08,
			0.10,
			0.05,
			0.08,
			0.02,
			0.08,
			0.07,
			0.15,
			0.05,
			0.18,
			0.02,
		},

		PredictedCoarseClass: domain.CoarseClassPulsating,

		PredictedLeafClass: domain.LeafClassCataclysmic,

		CompletedAt: time.Date(
			2026,
			time.August,
			5,
			8,
			30,
			0,
			123000000,
			time.FixedZone("UTC+8", 8*60*60),
		),
	}
}

func stringPointer(value string) *string {
	return &value
}
