package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkaadapter "github.com/ZChen470/variable-star-classification/internal/adapter/kafka"
	modelbundleadapter "github.com/ZChen470/variable-star-classification/internal/adapter/modelbundle"
	postgresadapter "github.com/ZChen470/variable-star-classification/internal/adapter/postgres"
	tritonadapter "github.com/ZChen470/variable-star-classification/internal/adapter/triton"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	serviceName           = "classifier-worker"
	tritonHTTPTimeout     = 10 * time.Second
	tritonMaxResponseSize = int64(1024 * 1024)
	lightCurveHTTPTimeout = 10 * time.Second
)

var classificationRetryDelays = []time.Duration{
	100 * time.Millisecond,
	300 * time.Millisecond,
}

func main() {
	logger := logging.NewJSON(os.Stderr, serviceName)
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error(
			"service failed",
			"operation", "run",
			"error", err,
		)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	config, err := loadClassifierWorkerConfig(os.LookupEnv)
	if err != nil {
		return fmt.Errorf("load classifier worker config: %w", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// 一个进程只加载一次不可变 Serving Manifest。
	servingResolver, err := modelbundleadapter.NewFileServingBundleResolver(
		config.modelBundleManifestPath,
	)
	if err != nil {
		return fmt.Errorf("load serving bundle manifest: %w", err)
	}

	// MODEL_BUNDLE_VERSION 同时作为启动期部署身份门禁。
	servingBundle, err := servingResolver.ResolveServingBundle(
		ctx,
		config.modelBundleVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"resolve configured serving bundle %q: %w",
			config.modelBundleVersion,
			err,
		)
	}

	modelBundleResolver := &servingBackedModelBundleResolver{
		serving: servingResolver,
	}

	// -------------------------
	// LightCurveRepository
	// -------------------------
	lightCurveHTTPClient := &http.Client{
		Timeout: lightCurveHTTPTimeout,
	}
	lightCurveRepository, err := newLightCurveRepository(
		config.lightCurveBaseURL,
		lightCurveHTTPClient,
	)
	if err != nil {
		return fmt.Errorf("create LightCurve repository: %w", err)
	}

	// ----------------------------------
	// PostgreSQL: 只用于查询兼容历史粗概率
	// ----------------------------------
	pool, err := pgxpool.New(ctx, config.postgresDSN)
	if err != nil {
		return fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}

	classificationRepository := postgresadapter.NewClassificationRepository(pool)

	// ------------------
	// Input preparation
	// ------------------
	lightCurveReader, err := application.NewLightCurveRevisionReader(
		lightCurveRepository,
	)
	if err != nil {
		return fmt.Errorf("create light curve revision reader: %w", err)
	}

	coarseModeSelector, err := application.NewCoarseModeSelector(
		modelBundleResolver,
		classificationRepository,
	)
	if err != nil {
		return fmt.Errorf("create coarse mode selector: %w", err)
	}

	inputPreparer, err := application.NewClassificationInputPreparer(
		lightCurveReader,
		coarseModeSelector,
	)
	if err != nil {
		return fmt.Errorf("create classification input preparer: %w", err)
	}

	// ----------------
	// Triton
	// ----------------
	tritonHTTPClient := &http.Client{
		Timeout: tritonHTTPTimeout,
	}

	tritonClient, err := tritonadapter.NewClient(
		config.tritonBaseURL,
		tritonHTTPClient,
		tritonMaxResponseSize,
	)
	if err != nil {
		return fmt.Errorf("create Triton client: %w", err)
	}

	// 启动时验证精确 model name / version 的 ready、metadata、config。
	contractGate, err := tritonadapter.NewModelContractGate(tritonClient)
	if err != nil {
		return fmt.Errorf("create Triton contract gate: %w", err)
	}

	if err := contractGate.Verify(ctx, servingBundle.Entrypoint); err != nil {
		return fmt.Errorf("verify Triton serving contract: %w", err)
	}

	classifier, err := tritonadapter.NewVariableStarClassifier(
		tritonClient,
		servingBundle.Entrypoint,
	)
	if err != nil {
		return fmt.Errorf("create variable star classifier: %w", err)
	}

	// -----------
	// Kafka
	// -----------
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
		kgo.ConsumerGroup(config.kafkaConsumerGroup),
		kgo.ConsumeTopics(config.classificationCommandTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer kafkaClient.CloseAllowingRebalance()

	publisher := kafkaadapter.NewPublisher(kafkaClient)

	// ------------------------------
	// Worker → Retry → Command DLQ
	// ------------------------------
	worker, err := application.NewClassificationWorkerHandler(
		config.classificationCommandTopic,
		config.classificationResultTopic,
		inputPreparer,
		servingResolver,
		classifier,
		publisher,
		time.Now,
	)
	if err != nil {
		return fmt.Errorf("create classification worker: %w", err)
	}

	handler, err := application.NewClassificationCommandHandler(
		worker,
		classificationRetryDelays,
		config.classificationCommandDLQTopic,
		publisher,
	)
	if err != nil {
		return fmt.Errorf("create classification command handler: %w", err)
	}

	runner, err := kafkaadapter.NewConsumerRunner(
		kafkaClient,
		handler,
	)
	if err != nil {
		return fmt.Errorf("create kafka consumer runner: %w", err)
	}

	logger.InfoContext(
		ctx,
		"service started",
		"operation", "startup",
		"kafka_client_id", config.kafkaClientID,
		"kafka_consumer_group", config.kafkaConsumerGroup,
		"command_topic", config.classificationCommandTopic,
		"result_topic", config.classificationResultTopic,
		"command_dlq_topic", config.classificationCommandDLQTopic,
		"model_bundle_version", config.modelBundleVersion,
	)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf(
			"run classifier worker: %w",
			err,
		)
	}

	shutdownReason := "consumer_runner_stopped"
	if ctx.Err() != nil {
		shutdownReason = "context_cancelled"
	}

	logger.Info(
		"service stopped",
		"operation", "shutdown",
		"shutdown_reason", shutdownReason,
	)

	return nil
}
