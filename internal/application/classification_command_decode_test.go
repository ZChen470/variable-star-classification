package application_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testCommandInputTopic = "astro.classification.commands.v1"

func TestDecodeClassificationCommandMessage(t *testing.T) {
	command := validClassificationCommandProto(t)
	message := classificationCommandInboundMessage(t, command)

	message.Timestamp = time.Date(
		2030,
		time.January,
		2,
		3,
		4,
		5,
		0,
		time.UTC,
	)

	got, err := application.DecodeClassificationCommandMessage(
		testCommandInputTopic,
		message,
	)
	if err != nil {
		t.Fatalf(
			"DecodeClassificationCommandMessage() error = %v",
			err,
		)
	}

	want := application.ClassificationCommandInput{
		JobID: domain.JobID(command.GetJobId()),

		ObjectID: command.GetObjectId(),

		CandidateRevision: command.GetCandidateRevision(),

		LightCurveRevision: command.GetLightCurveRevision(),

		DeclaredEligibleEpochCount: command.GetDeclaredEligibleEpochCount(),

		ModelBundleVersion: command.GetModelBundleVersion(),

		ExecutionMode: domain.ExecutionModeProduction,

		Priority: application.ClassificationPriorityRealtime,

		CreatedAt: command.GetCreatedAt().AsTime(),

		TraceContext: application.TraceContext{
			TraceID:       "trace-001",
			CorrelationID: "correlation-001",
			CausationID:   "candidate-event-001",
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"decoded input mismatch:\ngot  = %#v\nwant = %#v",
			got,
			want,
		)
	}
}

func TestDecodeClassificationCommandMessageMapsExecutionModes(
	t *testing.T,
) {
	tests := []struct {
		name      string
		protoMode classificationv1.ExecutionMode
		wantMode  domain.ExecutionMode
	}{
		{
			name: "production",
			protoMode: classificationv1.
				ExecutionMode_EXECUTION_MODE_PRODUCTION,
			wantMode: domain.ExecutionModeProduction,
		},
		{
			name: "shadow",
			protoMode: classificationv1.
				ExecutionMode_EXECUTION_MODE_SHADOW,
			wantMode: domain.ExecutionModeShadow,
		},
		{
			name: "reprocess",
			protoMode: classificationv1.
				ExecutionMode_EXECUTION_MODE_REPROCESS,
			wantMode: domain.ExecutionModeReprocess,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := validClassificationCommandProto(t)
			command.ExecutionMode = test.protoMode

			setClassificationCommandJobID(
				t,
				command,
				test.wantMode,
			)

			got, err :=
				application.DecodeClassificationCommandMessage(
					testCommandInputTopic,
					classificationCommandInboundMessage(
						t,
						command,
					),
				)
			if err != nil {
				t.Fatalf(
					"DecodeClassificationCommandMessage() error = %v",
					err,
				)
			}

			if got.ExecutionMode != test.wantMode {
				t.Fatalf(
					"ExecutionMode = %d, want %d",
					got.ExecutionMode,
					test.wantMode,
				)
			}
		})
	}
}

func TestDecodeClassificationCommandMessageIgnoresKafkaTimestamp(
	t *testing.T,
) {
	command := validClassificationCommandProto(t)

	firstMessage :=
		classificationCommandInboundMessage(t, command)

	secondMessage :=
		classificationCommandInboundMessage(t, command)

	secondMessage.Timestamp = time.Date(
		2035,
		time.June,
		7,
		8,
		9,
		10,
		0,
		time.UTC,
	)

	first, err :=
		application.DecodeClassificationCommandMessage(
			testCommandInputTopic,
			firstMessage,
		)
	if err != nil {
		t.Fatalf("first decode error = %v", err)
	}

	second, err :=
		application.DecodeClassificationCommandMessage(
			testCommandInputTopic,
			secondMessage,
		)
	if err != nil {
		t.Fatalf("second decode error = %v", err)
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatalf(
			"Kafka timestamp changed application input:\nfirst=%#v\nsecond=%#v",
			first,
			second,
		)
	}
}

func TestDecodeClassificationCommandMessagePermanentErrors(
	t *testing.T,
) {
	tests := []struct {
		name string

		mutateCommand func(
			*classificationv1.ClassificationCommand,
		)

		mutateMessage func(
			*application.InboundMessage,
		)

		wantCode  application.ClassificationCommandErrorCode
		wantField string
	}{
		{
			name: "unexpected topic",
			mutateMessage: func(
				message *application.InboundMessage,
			) {
				message.Topic = "unexpected.commands.v1"
			},
			wantCode: application.
				ClassificationCommandErrorCodeUnexpectedTopic,
			wantField: "topic",
		},
		{
			name: "missing key",
			mutateMessage: func(
				message *application.InboundMessage,
			) {
				message.Key = nil
			},
			wantCode: application.
				ClassificationCommandErrorCodeMissingKey,
			wantField: "key",
		},
		{
			name: "malformed protobuf",
			mutateMessage: func(
				message *application.InboundMessage,
			) {
				message.Value = []byte{0xff, 0xff}
			},
			wantCode: application.
				ClassificationCommandErrorCodeMalformedProto,
			wantField: "value",
		},
		{
			name: "empty job ID",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.JobId = ""
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "job_id",
		},
		{
			name: "empty object ID",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.ObjectId = ""
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "object_id",
		},
		{
			name: "invalid candidate revision",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.CandidateRevision = 0
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "candidate_revision",
		},
		{
			name: "invalid light curve revision",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.LightCurveRevision = 0
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "light_curve_revision",
		},
		{
			name: "insufficient epoch count",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.DeclaredEligibleEpochCount = 2
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "declared_eligible_epoch_count",
		},
		{
			name: "invalid bundle version",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.ModelBundleVersion = " bundle-v1"
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "model_bundle_version",
		},
		{
			name: "unsupported execution mode",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.ExecutionMode =
					classificationv1.ExecutionMode(99)
			},
			wantCode: application.
				ClassificationCommandErrorCodeExecutionMode,
			wantField: "execution_mode",
		},
		{
			name: "unsupported priority",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.Priority = classificationv1.
					ClassificationPriority_CLASSIFICATION_PRIORITY_NORMAL
			},
			wantCode: application.
				ClassificationCommandErrorCodePriority,
			wantField: "priority",
		},
		{
			name: "missing created at",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.CreatedAt = nil
			},
			wantCode: application.
				ClassificationCommandErrorCodeInvalidField,
			wantField: "created_at",
		},
		{
			name: "unsupported deadline",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.DeadlineAt = timestamppb.New(
					time.Date(
						2026,
						time.August,
						6,
						0,
						0,
						0,
						0,
						time.UTC,
					),
				)
			},
			wantCode: application.
				ClassificationCommandErrorCodeDeadline,
			wantField: "deadline_at",
		},
		{
			name: "key mismatch",
			mutateMessage: func(
				message *application.InboundMessage,
			) {
				message.Key = []byte("OTHER-OBJECT")
			},
			wantCode: application.
				ClassificationCommandErrorCodeKeyMismatch,
			wantField: "key",
		},
		{
			name: "job ID mismatch",
			mutateCommand: func(
				command *classificationv1.ClassificationCommand,
			) {
				command.JobId =
					"22222222-2222-2222-2222-222222222222"
			},
			wantCode: application.
				ClassificationCommandErrorCodeJobIDMismatch,
			wantField: "job_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command :=
				validClassificationCommandProto(t)

			message :=
				classificationCommandInboundMessage(
					t,
					command,
				)

			if test.mutateCommand != nil {
				test.mutateCommand(command)

				value, err := proto.Marshal(command)
				if err != nil {
					t.Fatalf(
						"proto.Marshal() error = %v",
						err,
					)
				}

				message.Value = value
			}

			if test.mutateMessage != nil {
				test.mutateMessage(&message)
			}

			got, err :=
				application.DecodeClassificationCommandMessage(
					testCommandInputTopic,
					message,
				)

			var permanent *application.
				PermanentClassificationCommandError

			if !errors.As(err, &permanent) {
				t.Fatalf(
					"error = %v, want PermanentClassificationCommandError",
					err,
				)
			}

			if permanent.Code != test.wantCode {
				t.Fatalf(
					"error code = %q, want %q",
					permanent.Code,
					test.wantCode,
				)
			}

			if permanent.Field != test.wantField {
				t.Fatalf(
					"error field = %q, want %q",
					permanent.Field,
					test.wantField,
				)
			}

			if !reflect.DeepEqual(
				got,
				application.ClassificationCommandInput{},
			) {
				t.Fatalf(
					"input = %#v, want zero",
					got,
				)
			}
		})
	}
}

func TestDecodeClassificationCommandMessageConfigError(
	t *testing.T,
) {
	command := validClassificationCommandProto(t)

	_, err :=
		application.DecodeClassificationCommandMessage(
			"",
			classificationCommandInboundMessage(
				t,
				command,
			),
		)

	if err == nil {
		t.Fatal("error = nil, want non-nil")
	}

	var permanent *application.
		PermanentClassificationCommandError

	if errors.As(err, &permanent) {
		t.Fatalf(
			"error = %v, must not be permanent",
			err,
		)
	}
}

func validClassificationCommandProto(
	t *testing.T,
) *classificationv1.ClassificationCommand {
	t.Helper()

	command := &classificationv1.ClassificationCommand{
		ObjectId:           "OBJ-0001",
		CandidateRevision:  7,
		LightCurveRevision: 101,

		DeclaredEligibleEpochCount: 21,

		ModelBundleVersion: "bundle-v1",

		ExecutionMode: classificationv1.
			ExecutionMode_EXECUTION_MODE_PRODUCTION,

		Priority: classificationv1.
			ClassificationPriority_CLASSIFICATION_PRIORITY_REALTIME,

		CreatedAt: timestamppb.New(
			time.Date(
				2026,
				time.August,
				5,
				9,
				30,
				0,
				123000000,
				time.UTC,
			),
		),

		TraceContext: &classificationv1.TraceContext{
			TraceId:       "trace-001",
			CorrelationId: "correlation-001",
			CausationId:   "candidate-event-001",
		},
	}

	setClassificationCommandJobID(
		t,
		command,
		domain.ExecutionModeProduction,
	)

	return command
}

func setClassificationCommandJobID(
	t *testing.T,
	command *classificationv1.ClassificationCommand,
	executionMode domain.ExecutionMode,
) {
	t.Helper()

	jobID, err := domain.GenerateJobID(
		domain.JobIdentity{
			ObjectID: command.GetObjectId(),

			LightCurveRevision: command.GetLightCurveRevision(),

			ModelBundleVersion: command.GetModelBundleVersion(),

			ExecutionMode: executionMode,
		},
	)
	if err != nil {
		t.Fatalf(
			"GenerateJobID() error = %v",
			err,
		)
	}

	command.JobId = string(jobID)
}

func classificationCommandInboundMessage(
	t *testing.T,
	command *classificationv1.ClassificationCommand,
) application.InboundMessage {
	t.Helper()

	value, err := proto.Marshal(command)
	if err != nil {
		t.Fatalf(
			"proto.Marshal() error = %v",
			err,
		)
	}

	return application.InboundMessage{
		Topic: testCommandInputTopic,
		Key:   []byte(command.GetObjectId()),
		Value: value,

		Headers: []application.MessageHeader{
			{
				Key:   "traceparent",
				Value: []byte("00-test"),
			},
		},
	}
}
