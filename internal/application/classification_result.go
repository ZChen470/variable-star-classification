package application

import (
	"errors"
	"fmt"
	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrInvalidClassificationResultMessage 表示成功 Run 无法映射为合法
	// ClassificationResult 消息。
	ErrInvalidClassificationResultMessage = errors.New(
		"invalid classification result message",
	)
)

// BuildClassificationResultMessage 将一次成功 ClassificationRun 映射为
// 尚未发布的 ClassificationResult Kafka 消息。
//
// 本函数：
//   - 不执行概率和、融合公式或 REUSE 概率一致性校验；
//   - 不填充 Timing；
//   - 使用 object_id 作为 Kafka Key；
//   - 使用 completed_at 作为 Kafka Record Timestamp；
//   - 原样深拷贝 Kafka Headers；
//   - 使用确定性 Protobuf 序列化。
func BuildClassificationResultMessage(resultTopic string, run domain.ClassificationRun, traceContext TraceContext, headers []MessageHeader) (OutboundMessage, error) {
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

		CoarseProbabilities: mapCoarseProbabilities(
			run.CoarseProbabilities,
		),
		FineConditionalProbabilities: mapFineProbabilities(
			run.FineConditionalProbabilities,
		),
		LeafProbabilities: mapLeafProbabilities(
			run.LeafProbabilities,
		),

		PredictedCoarseClass: classificationv1.CoarseClass(
			run.PredictedCoarseClass,
		),
		PredictedLeafClass: classificationv1.LeafClass(
			run.PredictedLeafClass,
		),

		CompletedAt: timestamppb.New(run.CompletedAt),

		TraceContext: mapResultTraceContext(traceContext),

		ExecutionMode: classificationv1.ExecutionMode(
			run.ExecutionMode,
		),
	}

	if run.CoarseSourceRunID != nil {
		sourceRunID := string(*run.CoarseSourceRunID)
		result.CoarseSourceRunId = &sourceRunID
	}

	value, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(result)
	if err != nil {
		return OutboundMessage{}, fmt.Errorf(
			"marshal ClassificationResult: %w",
			err,
		)
	}

	return OutboundMessage{
		Topic:     resultTopic,
		Key:       []byte(run.ObjectID),
		Value:     value,
		Headers:   cloneResultHeaders(headers),
		Timestamp: run.CompletedAt,
	}, nil
}

func cloneResultHeaders(headers []MessageHeader) []MessageHeader {
	if headers == nil {
		return nil
	}
	cloned := make([]MessageHeader, len(headers))
	for i, header := range headers {
		cloned[i].Key = header.Key

		if header.Value != nil {
			cloned[i].Value = make([]byte, len(header.Value))
			copy(cloned[i].Value, header.Value)
		}
	}
	return cloned
}

func mapResultTraceContext(traceCtx TraceContext) *classificationv1.TraceContext {
	if traceCtx.TraceID == "" && traceCtx.CorrelationID == "" && traceCtx.CausationID == "" {
		return nil
	}
	return &classificationv1.TraceContext{
		TraceId:       traceCtx.TraceID,
		CorrelationId: traceCtx.CorrelationID,
		CausationId:   traceCtx.CausationID,
	}
}

func mapCoarseProbabilities(probs [domain.CoarseProbabilityCount]float32) *classificationv1.CoarseProbabilities {
	return &classificationv1.CoarseProbabilities{
		Rotating:        probs[0],
		Cataclysmic:     probs[1],
		EclipsingBinary: probs[2],
		LongPeriod:      probs[3],
		Pulsating:       probs[4],
		RrLyrae:         probs[5],
		Supernova:       probs[6],
	}
}

func mapFineProbabilities(probs [domain.ConditionalFineProbabilityCount]float32) *classificationv1.FineConditionalProbabilities {
	return &classificationv1.FineConditionalProbabilities{
		EwGivenEclipsingBinary: probs[0],
		EaGivenEclipsingBinary: probs[1],
		ByDraGivenRotating:     probs[2],
		RsCvnGivenRotating:     probs[3],
		RrabGivenRrLyrae:       probs[4],
		RrcGivenRrLyrae:        probs[5],
		SrGivenLongPeriod:      probs[6],
		MiraGivenLongPeriod:    probs[7],
		DsctGivenPulsating:     probs[8],
		CepGivenPulsating:      probs[9],
	}
}

func mapLeafProbabilities(probabilities [domain.LeafProbabilityCount]float32) *classificationv1.LeafProbabilities {
	return &classificationv1.LeafProbabilities{
		Ew:          probabilities[0],
		Ea:          probabilities[1],
		ByDra:       probabilities[2],
		RsCvn:       probabilities[3],
		Rrab:        probabilities[4],
		Rrc:         probabilities[5],
		Sr:          probabilities[6],
		Mira:        probabilities[7],
		Dsct:        probabilities[8],
		Cep:         probabilities[9],
		Cataclysmic: probabilities[10],
		Supernova:   probabilities[11],
	}
}
