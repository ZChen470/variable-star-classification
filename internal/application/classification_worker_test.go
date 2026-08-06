package application_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakeclassifier"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakelightcurve"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakemodelbundle"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakeservingbundle"
	"google.golang.org/protobuf/proto"
)

const testWorkerResultTopic = "astro.classification.results.v1"

var (
	errWorkerLightCurve = errors.New(
		"worker light curve failure",
	)
	errWorkerServingBundle = errors.New(
		"worker serving bundle failure",
	)
	errWorkerClassifier = errors.New(
		"worker classifier failure",
	)
	errWorkerPublisher = errors.New(
		"worker publisher failure",
	)
)

func TestClassificationWorkerHandlerPublishesResult(
	t *testing.T,
) {
	fixture := newClassificationWorkerFixture(
		t,
		classificationWorkerFixtureOptions{},
	)

	err := fixture.handler.Handle(
		context.Background(),
		fixture.message,
	)
	if err != nil {
		t.Fatalf(
			"Handle() error = %v",
			err,
		)
	}

	if calls := fixture.lightCurveRepository.Calls(); len(calls) != 1 {
		t.Fatalf(
			"light curve call count = %d, want 1",
			len(calls),
		)
	}

	if calls := fixture.modelBundleResolver.Calls(); len(calls) != 1 ||
		calls[0] != fixture.command.GetModelBundleVersion() {
		t.Fatalf(
			"model bundle calls = %#v, want [%q]",
			calls,
			fixture.command.GetModelBundleVersion(),
		)
	}

	if calls := fixture.servingBundleResolver.Calls(); len(calls) != 1 ||
		calls[0] != fixture.command.GetModelBundleVersion() {
		t.Fatalf(
			"serving bundle calls = %#v, want [%q]",
			calls,
			fixture.command.GetModelBundleVersion(),
		)
	}

	classifierCalls := fixture.classifier.Calls()
	if len(classifierCalls) != 1 {
		t.Fatalf(
			"classifier call count = %d, want 1",
			len(classifierCalls),
		)
	}

	requestIDs := fixture.classifier.RequestIDs()
	if len(requestIDs) != 1 {
		t.Fatalf(
			"classifier request ID count = %d, want 1",
			len(requestIDs),
		)
	}

	if requestIDs[0] != fixture.command.GetJobId() {
		t.Fatalf(
			"classifier request ID = %q, want job_id %q",
			requestIDs[0],
			fixture.command.GetJobId(),
		)
	}

	classifierInput := classifierCalls[0]

	wantTimeMJD := []float64{
		60421.1,
		60422.2,
		60423.3,
	}
	if !equalFloat64Slices(
		classifierInput.TimeMJD,
		wantTimeMJD,
	) {
		t.Fatalf(
			"classifier TimeMJD = %#v, want %#v",
			classifierInput.TimeMJD,
			wantTimeMJD,
		)
	}

	if classifierInput.CoarseMode !=
		application.CoarseModeComputeCurrent {
		t.Fatalf(
			"classifier CoarseMode = %d, want %d",
			classifierInput.CoarseMode,
			application.CoarseModeComputeCurrent,
		)
	}

	published := fixture.publisher.Calls()
	if len(published) != 1 {
		t.Fatalf(
			"publisher call count = %d, want 1",
			len(published),
		)
	}

	message := published[0]

	if message.Topic != testWorkerResultTopic {
		t.Fatalf(
			"result topic = %q, want %q",
			message.Topic,
			testWorkerResultTopic,
		)
	}

	if !bytes.Equal(
		message.Key,
		[]byte(fixture.command.GetObjectId()),
	) {
		t.Fatalf(
			"result key = %q, want %q",
			message.Key,
			fixture.command.GetObjectId(),
		)
	}

	if !message.Timestamp.Equal(fixture.completedAt) {
		t.Fatalf(
			"result timestamp = %v, want %v",
			message.Timestamp,
			fixture.completedAt,
		)
	}

	var result classificationv1.ClassificationResult
	if err := proto.Unmarshal(
		message.Value,
		&result,
	); err != nil {
		t.Fatalf(
			"unmarshal ClassificationResult: %v",
			err,
		)
	}

	expectedRunID, err := domain.GenerateRunID(
		domain.JobID(fixture.command.GetJobId()),
	)
	if err != nil {
		t.Fatalf(
			"GenerateRunID() error = %v",
			err,
		)
	}

	if result.GetRunId() != string(expectedRunID) {
		t.Fatalf(
			"result run_id = %q, want %q",
			result.GetRunId(),
			expectedRunID,
		)
	}

	if result.GetJobId() != fixture.command.GetJobId() {
		t.Fatalf(
			"result job_id = %q, want %q",
			result.GetJobId(),
			fixture.command.GetJobId(),
		)
	}

	if result.GetObjectId() !=
		fixture.command.GetObjectId() {
		t.Fatalf(
			"result object_id = %q, want %q",
			result.GetObjectId(),
			fixture.command.GetObjectId(),
		)
	}

	if result.GetLightCurveRevision() !=
		fixture.command.GetLightCurveRevision() {
		t.Fatalf(
			"result light_curve_revision = %d, want %d",
			result.GetLightCurveRevision(),
			fixture.command.GetLightCurveRevision(),
		)
	}

	if result.GetVersions().
		GetModelBundleVersion() !=
		fixture.command.GetModelBundleVersion() {
		t.Fatalf(
			"result model_bundle_version = %q, want %q",
			result.GetVersions().
				GetModelBundleVersion(),
			fixture.command.GetModelBundleVersion(),
		)
	}

	if result.GetCoarseSourceType() !=
		classificationv1.
			CoarseSourceType_COARSE_SOURCE_COMPUTED_CURRENT {
		t.Fatalf(
			"result coarse source = %v",
			result.GetCoarseSourceType(),
		)
	}

	if !result.GetXgboostExecuted() {
		t.Fatal(
			"result xgboost_executed = false, want true",
		)
	}

	if !result.GetCompletedAt().
		AsTime().
		Equal(fixture.completedAt) {
		t.Fatalf(
			"result completed_at = %v, want %v",
			result.GetCompletedAt().AsTime(),
			fixture.completedAt,
		)
	}

	if result.GetTraceContext().GetTraceId() !=
		fixture.command.GetTraceContext().GetTraceId() {
		t.Fatalf(
			"result trace_id = %q, want %q",
			result.GetTraceContext().GetTraceId(),
			fixture.command.GetTraceContext().
				GetTraceId(),
		)
	}

	if len(message.Headers) != len(fixture.message.Headers) {
		t.Fatalf(
			"result Header count = %d, want %d",
			len(message.Headers),
			len(fixture.message.Headers),
		)
	}
}

func TestClassificationWorkerHandlerStopsAfterFailure(
	t *testing.T,
) {
	tests := []struct {
		name string

		options classificationWorkerFixtureOptions

		mutateMessage func(*application.InboundMessage)

		wantClass     application.ClassificationWorkerErrorClass
		wantCode      application.ClassificationWorkerErrorCode
		wantOperation application.ClassificationWorkerOperation

		wantPermanentCommandError bool
		wantCause                 error

		wantLightCurveCalls    int
		wantModelBundleCalls   int
		wantServingBundleCalls int
		wantClassifierCalls    int
		wantPublisherCalls     int
	}{
		{
			name: "command decode failure",
			mutateMessage: func(
				message *application.InboundMessage,
			) {
				message.Value = []byte{0xff, 0xff}
			},

			wantClass: application.
				ClassificationWorkerErrorClassPermanent,

			wantCode: application.ClassificationWorkerErrorCode(
				application.
					ClassificationCommandErrorCodeMalformedProto,
			),

			wantOperation: application.
				ClassificationWorkerOperationDecodeCommand,

			wantPermanentCommandError: true,
		},
		{
			name: "fixed revision read failure",
			options: classificationWorkerFixtureOptions{
				lightCurveError: errWorkerLightCurve,
			},

			wantClass: application.
				ClassificationWorkerErrorClassRetryable,

			wantCode: application.
				ClassificationWorkerErrorCodeDependencyUnavailable,

			wantOperation: application.
				ClassificationWorkerOperationPrepareInput,

			wantCause: errWorkerLightCurve,

			wantLightCurveCalls: 1,
		},
		{
			name: "input preparation failure",
			options: classificationWorkerFixtureOptions{
				invalidLightCurve: true,
			},

			wantClass: application.
				ClassificationWorkerErrorClassPermanent,

			wantCode: application.
				ClassificationWorkerErrorCodeLightCurveInvalid,

			wantOperation: application.
				ClassificationWorkerOperationPrepareInput,

			wantLightCurveCalls: 1,
		},
		{
			name: "serving bundle resolution failure",
			options: classificationWorkerFixtureOptions{
				servingBundleError: errWorkerServingBundle,
			},

			wantClass: application.
				ClassificationWorkerErrorClassRetryable,

			wantCode: application.
				ClassificationWorkerErrorCodeDependencyUnavailable,

			wantOperation: application.
				ClassificationWorkerOperationResolveBundle,

			wantCause: errWorkerServingBundle,

			wantLightCurveCalls:    1,
			wantModelBundleCalls:   1,
			wantServingBundleCalls: 1,
		},
		{
			name: "classifier failure",
			options: classificationWorkerFixtureOptions{
				classifierError: errWorkerClassifier,
			},

			wantClass: application.
				ClassificationWorkerErrorClassRetryable,

			wantCode: application.
				ClassificationWorkerErrorCodeDependencyUnavailable,

			wantOperation: application.
				ClassificationWorkerOperationClassify,

			wantCause: errWorkerClassifier,

			wantLightCurveCalls:    1,
			wantModelBundleCalls:   1,
			wantServingBundleCalls: 1,
			wantClassifierCalls:    1,
		},
		{
			name: "run construction failure",
			options: classificationWorkerFixtureOptions{
				servingBundleVersion: "different-bundle",
			},

			wantClass: application.
				ClassificationWorkerErrorClassPermanent,

			wantCode: application.
				ClassificationWorkerErrorCodeResultInvalid,

			wantOperation: application.
				ClassificationWorkerOperationBuildRun,

			wantCause: application.
				ErrInvalidClassificationRunBuild,

			wantLightCurveCalls:    1,
			wantModelBundleCalls:   1,
			wantServingBundleCalls: 1,
			wantClassifierCalls:    1,
		},
		{
			name: "result publish failure",
			options: classificationWorkerFixtureOptions{
				publisherError: errWorkerPublisher,
			},

			wantClass: application.
				ClassificationWorkerErrorClassRetryable,

			wantCode: application.
				ClassificationWorkerErrorCodePublishFailed,

			wantOperation: application.
				ClassificationWorkerOperationPublishResult,

			wantCause: errWorkerPublisher,

			wantLightCurveCalls:    1,
			wantModelBundleCalls:   1,
			wantServingBundleCalls: 1,
			wantClassifierCalls:    1,
			wantPublisherCalls:     1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newClassificationWorkerFixture(
				t,
				test.options,
			)

			message := fixture.message

			if test.mutateMessage != nil {
				test.mutateMessage(&message)
			}

			err := fixture.handler.Handle(
				context.Background(),
				message,
			)
			if err == nil {
				t.Fatal(
					"Handle() error = nil, want non-nil",
				)
			}

			var workerError *application.
				ClassificationWorkerError

			if !errors.As(err, &workerError) {
				t.Fatalf(
					"Handle() error = %v, want ClassificationWorkerError",
					err,
				)
			}

			if workerError.Class != test.wantClass {
				t.Fatalf(
					"worker error class = %s, want %s",
					workerError.Class,
					test.wantClass,
				)
			}

			if workerError.Code != test.wantCode {
				t.Fatalf(
					"worker error code = %q, want %q",
					workerError.Code,
					test.wantCode,
				)
			}

			if workerError.Operation !=
				test.wantOperation {
				t.Fatalf(
					"worker error operation = %q, want %q",
					workerError.Operation,
					test.wantOperation,
				)
			}

			if test.wantCause != nil &&
				!errors.Is(err, test.wantCause) {
				t.Fatalf(
					"Handle() error = %v, want cause %v",
					err,
					test.wantCause,
				)
			}

			if test.wantPermanentCommandError {
				var permanent *application.
					PermanentClassificationCommandError

				if !errors.As(err, &permanent) {
					t.Fatalf(
						"Handle() error = %v, want PermanentClassificationCommandError",
						err,
					)
				}
			}

			assertWorkerCallCount(
				t,
				"light curve repository",
				len(
					fixture.
						lightCurveRepository.
						Calls(),
				),
				test.wantLightCurveCalls,
			)

			assertWorkerCallCount(
				t,
				"model bundle resolver",
				len(
					fixture.
						modelBundleResolver.
						Calls(),
				),
				test.wantModelBundleCalls,
			)

			assertWorkerCallCount(
				t,
				"serving bundle resolver",
				len(
					fixture.
						servingBundleResolver.
						Calls(),
				),
				test.wantServingBundleCalls,
			)

			assertWorkerCallCount(
				t,
				"classifier",
				len(
					fixture.
						classifier.
						Calls(),
				),
				test.wantClassifierCalls,
			)

			assertWorkerCallCount(
				t,
				"publisher",
				len(
					fixture.
						publisher.
						Calls(),
				),
				test.wantPublisherCalls,
			)
		})
	}
}

type classificationWorkerFixtureOptions struct {
	lightCurveError   error
	invalidLightCurve bool

	servingBundleError   error
	servingBundleVersion string

	classifierError error
	publisherError  error
}

type classificationWorkerFixture struct {
	handler *application.ClassificationWorkerHandler

	command *classificationv1.ClassificationCommand
	message application.InboundMessage

	completedAt time.Time

	lightCurveRepository  *fakelightcurve.Repository
	modelBundleResolver   *fakemodelbundle.Resolver
	servingBundleResolver *fakeservingbundle.Resolver
	classifier            *fakeclassifier.Classifier
	publisher             *classificationWorkerPublisher
}

func newClassificationWorkerFixture(
	t *testing.T,
	options classificationWorkerFixtureOptions,
) classificationWorkerFixture {
	t.Helper()

	command := validClassificationCommandProto(t)

	command.ObjectId = "OBJ-WORKER-001"
	command.CandidateRevision = 7
	command.LightCurveRevision = 101
	command.DeclaredEligibleEpochCount = 3
	command.ModelBundleVersion = "bundle-v1"
	command.ExecutionMode = classificationv1.
		ExecutionMode_EXECUTION_MODE_PRODUCTION

	setClassificationCommandJobID(
		t,
		command,
		domain.ExecutionModeProduction,
	)

	message := classificationCommandInboundMessage(
		t,
		command,
	)

	epochs := []domain.LightCurveEpoch{
		{
			ObservationTime: 60423.3,
			Magnitude:       17.53,
			MagnitudeError:  0.05,
		},
		{
			ObservationTime: 60421.1,
			Magnitude:       17.31,
			MagnitudeError:  0.03,
		},
		{
			ObservationTime: 60422.2,
			Magnitude:       17.42,
			MagnitudeError:  0.04,
		},
	}

	eligibleEpochCount := uint32(len(epochs))

	if options.invalidLightCurve {
		epochs = epochs[:2]
		eligibleEpochCount = 2
	}

	lightCurveRequest := fakelightcurve.Request{
		ObjectID: command.GetObjectId(),
		Revision: command.GetLightCurveRevision(),
	}

	lightCurveRepository := fakelightcurve.New(
		map[fakelightcurve.Request]fakelightcurve.Response{
			lightCurveRequest: {
				Revision: domain.LightCurveRevision{
					ObjectID: command.GetObjectId(),

					Revision: command.GetLightCurveRevision(),

					EligibleEpochCount: eligibleEpochCount,

					Epochs: epochs,
				},
				Err: options.lightCurveError,
			},
		},
	)

	modelBundleResolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			command.GetModelBundleVersion(): {
				Metadata: application.ModelBundleMetadata{
					ModelBundleVersion: command.GetModelBundleVersion(),
				},
			},
		},
	)

	inputPreparer := newClassificationInputPreparerForTest(
		t,
		lightCurveRepository,
		modelBundleResolver,
		&preparationCompatibleCoarseFinder{},
	)

	servingBundleVersion :=
		options.servingBundleVersion
	if servingBundleVersion == "" {
		servingBundleVersion =
			command.GetModelBundleVersion()
	}

	servingBundleResolver := fakeservingbundle.New(
		map[string]fakeservingbundle.Response{
			command.GetModelBundleVersion(): {
				Metadata: application.ServingBundleMetadata{
					ModelBundleVersion: servingBundleVersion,
				},
				Err: options.servingBundleError,
			},
		},
	)

	classifier := fakeclassifier.New(
		validClassificationWorkerOutput(),
		options.classifierError,
	)

	publisher := &classificationWorkerPublisher{
		err: options.publisherError,
	}

	completedAt := time.Date(
		2026,
		time.August,
		6,
		5,
		30,
		0,
		123000000,
		time.UTC,
	)

	handler, err :=
		application.NewClassificationWorkerHandler(
			testCommandInputTopic,
			testWorkerResultTopic,
			inputPreparer,
			servingBundleResolver,
			classifier,
			publisher,
			func() time.Time {
				return completedAt
			},
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationWorkerHandler() error = %v",
			err,
		)
	}

	return classificationWorkerFixture{
		handler: handler,

		command: command,
		message: message,

		completedAt: completedAt,

		lightCurveRepository: lightCurveRepository,

		modelBundleResolver: modelBundleResolver,

		servingBundleResolver: servingBundleResolver,

		classifier: classifier,
		publisher:  publisher,
	}
}

func validClassificationWorkerOutput() application.ClassificationOutput {
	return application.ClassificationOutput{
		CoarseProbabilities: [domain.CoarseProbabilityCount]float32{
			0.05,
			0.10,
			0.20,
			0.15,
			0.30,
			0.10,
			0.10,
		},

		ConditionalFineProbabilities: [domain.ConditionalFineProbabilityCount]float32{
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

		XGBoostExecuted: true,
	}
}

type classificationWorkerPublisher struct {
	err   error
	calls []application.OutboundMessage
}

var _ application.MessagePublisher = (*classificationWorkerPublisher)(nil)

func (publisher *classificationWorkerPublisher) Publish(
	ctx context.Context,
	message application.OutboundMessage,
) error {
	if ctx == nil {
		return errors.New("nil context")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	publisher.calls = append(
		publisher.calls,
		cloneClassificationWorkerMessage(message),
	)

	if publisher.err != nil {
		return publisher.err
	}

	return nil
}

func (publisher *classificationWorkerPublisher) Calls() []application.OutboundMessage {
	calls := make(
		[]application.OutboundMessage,
		len(publisher.calls),
	)

	for index, message := range publisher.calls {
		calls[index] =
			cloneClassificationWorkerMessage(message)
	}

	return calls
}

func cloneClassificationWorkerMessage(
	message application.OutboundMessage,
) application.OutboundMessage {
	cloned := message

	cloned.Key = append([]byte(nil), message.Key...)
	cloned.Value = append([]byte(nil), message.Value...)

	if message.Headers != nil {
		cloned.Headers = make(
			[]application.MessageHeader,
			len(message.Headers),
		)

		for index, header := range message.Headers {
			cloned.Headers[index].Key = header.Key

			if header.Value != nil {
				cloned.Headers[index].Value =
					append(
						[]byte(nil),
						header.Value...,
					)
			}
		}
	}

	return cloned
}

func assertWorkerCallCount(
	t *testing.T,
	dependency string,
	got int,
	want int,
) {
	t.Helper()

	if got != want {
		t.Fatalf(
			"%s call count = %d, want %d",
			dependency,
			got,
			want,
		)
	}
}

func equalFloat64Slices(
	first []float64,
	second []float64,
) bool {
	if len(first) != len(second) {
		return false
	}

	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}

	return true
}
