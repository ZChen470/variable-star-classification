package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	"github.com/ZChen470/variable-star-classification/internal/observability/commandmetrics"
	"github.com/ZChen470/variable-star-classification/internal/observability/httpmetrics"
	"github.com/ZChen470/variable-star-classification/internal/observability/kafkametrics"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/ZChen470/variable-star-classification/internal/observability/management"
	"github.com/ZChen470/variable-star-classification/internal/observability/postgresmetrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	serviceName                 = "classifier-worker"
	tritonHTTPTimeout           = 10 * time.Second
	tritonMaxResponseSize       = int64(1024 * 1024)
	lightCurveHTTPTimeout       = 10 * time.Second
	managementReadHeaderTimeout = 5 * time.Second
	managementShutdownTimeout   = 5 * time.Second
)

var classificationRetryDelays = []time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
	3200 * time.Millisecond,
	6400 * time.Millisecond,
	10 * time.Second,
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
	config, err := loadClassifierWorkerConfig(
		os.LookupEnv,
	)
	if err != nil {
		return fmt.Errorf(
			"load classifier worker config: %w",
			err,
		)
	}

	signalContext, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	runContext, cancelRun := context.WithCancel(
		signalContext,
	)
	defer cancelRun()

	registry := management.NewRegistry()

	kafkaMetrics, err := kafkametrics.New(registry)
	if err != nil {
		return fmt.Errorf(
			"create Kafka metrics hook: %w",
			err,
		)
	}
	rebalanceMetrics, err := kafkametrics.NewRebalanceObserver(registry)
	if err != nil {
		return fmt.Errorf("create Kafka rebalance metrics: %w", err)
	}

	httpMetrics, err := httpmetrics.New(registry)
	if err != nil {
		return fmt.Errorf(
			"create HTTP client metrics: %w",
			err,
		)
	}

	commandObserver, err := commandmetrics.New(registry)
	if err != nil {
		return fmt.Errorf(
			"create classification command metrics: %w",
			err,
		)
	}

	servingResolver, err :=
		modelbundleadapter.NewFileServingBundleResolver(
			config.modelBundleManifestPath,
		)
	if err != nil {
		return fmt.Errorf(
			"load serving bundle manifest: %w",
			err,
		)
	}

	servingBundle, err :=
		servingResolver.ResolveServingBundle(
			runContext,
			config.modelBundleVersion,
		)
	if err != nil {
		return fmt.Errorf(
			"resolve configured serving bundle %q: %w",
			config.modelBundleVersion,
			err,
		)
	}

	modelBundleResolver :=
		&servingBackedModelBundleResolver{
			serving: servingResolver,
		}

	lightCurveTransport, err := httpMetrics.WrapTransport(
		httpmetrics.TargetLightCurve,
		http.DefaultTransport,
	)
	if err != nil {
		return fmt.Errorf(
			"create LightCurve HTTP metrics transport: %w",
			err,
		)
	}

	lightCurveHTTPClient := &http.Client{
		Transport: lightCurveTransport,
		Timeout:   lightCurveHTTPTimeout,
	}

	lightCurveRepository, err := newLightCurveRepository(
		config.lightCurveBaseURL,
		lightCurveHTTPClient,
	)
	if err != nil {
		return fmt.Errorf(
			"create LightCurve repository: %w",
			err,
		)
	}

	pool, err := pgxpool.New(
		runContext,
		config.postgresDSN,
	)
	if err != nil {
		return fmt.Errorf(
			"create PostgreSQL pool: %w",
			err,
		)
	}
	defer pool.Close()

	if err := pool.Ping(runContext); err != nil {
		return fmt.Errorf(
			"ping PostgreSQL: %w",
			err,
		)
	}

	if _, err := postgresmetrics.New(
		registry,
		pool,
	); err != nil {
		return fmt.Errorf(
			"create PostgreSQL pool metrics: %w",
			err,
		)
	}

	classificationRepository :=
		postgresadapter.NewClassificationRepository(pool)

	lightCurveReader, err :=
		application.NewLightCurveRevisionReader(
			lightCurveRepository,
		)
	if err != nil {
		return fmt.Errorf(
			"create light curve revision reader: %w",
			err,
		)
	}

	coarseModeSelector, err :=
		application.NewCoarseModeSelector(
			modelBundleResolver,
			classificationRepository,
		)
	if err != nil {
		return fmt.Errorf(
			"create coarse mode selector: %w",
			err,
		)
	}

	inputPreparer, err :=
		application.NewClassificationInputPreparer(
			lightCurveReader,
			coarseModeSelector,
		)
	if err != nil {
		return fmt.Errorf(
			"create classification input preparer: %w",
			err,
		)
	}

	tritonTransport, err := httpMetrics.WrapTransport(
		httpmetrics.TargetTriton,
		http.DefaultTransport,
	)
	if err != nil {
		return fmt.Errorf(
			"create Triton HTTP metrics transport: %w",
			err,
		)
	}

	tritonHTTPClient := &http.Client{
		Transport: tritonTransport,
		Timeout:   tritonHTTPTimeout,
	}

	tritonClient, err := tritonadapter.NewClient(
		config.tritonBaseURL,
		tritonHTTPClient,
		tritonMaxResponseSize,
	)
	if err != nil {
		return fmt.Errorf(
			"create Triton client: %w",
			err,
		)
	}

	contractGate, err :=
		tritonadapter.NewModelContractGate(
			tritonClient,
		)
	if err != nil {
		return fmt.Errorf(
			"create Triton contract gate: %w",
			err,
		)
	}

	if err := contractGate.Verify(
		runContext,
		servingBundle.Entrypoint,
	); err != nil {
		return fmt.Errorf(
			"verify Triton serving contract: %w",
			err,
		)
	}

	classifier, err :=
		tritonadapter.NewVariableStarClassifier(
			tritonClient,
			servingBundle.Entrypoint,
		)
	if err != nil {
		return fmt.Errorf(
			"create variable star classifier: %w",
			err,
		)
	}

	rebalanceYield := kafkaadapter.NewRebalanceYield()

	kafkaOptions := []kgo.Opt{
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
		kgo.ConsumerGroup(config.kafkaConsumerGroup),
		kgo.ConsumeTopics(config.classificationCommandTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsCallbackBlocked(func(ctx context.Context, client *kgo.Client) {
			rebalanceMetrics.OnPartitionsCallbackBlocked(ctx, client)
			rebalanceYield.Request()
		}),
		kgo.WithHooks(kafkaMetrics),
	}

	if config.kafkaSASLUsername != "" {
		mechanism, err := kafkaadapter.NewSCRAMSHA256Mechanism(
			config.kafkaSASLUsername,
			config.kafkaSASLPassword,
		)
		if err != nil {
			return fmt.Errorf("create Kafka SASL mechanism: %w", err)
		}

		kafkaOptions = append(kafkaOptions, kgo.SASL(mechanism))
	}

	kafkaClient, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer kafkaClient.CloseAllowingRebalance()

	publisher := kafkaadapter.NewPublisher(kafkaClient)

	worker, err :=
		application.NewClassificationWorkerHandler(
			config.classificationCommandTopic,
			config.classificationResultTopic,
			inputPreparer,
			servingResolver,
			classifier,
			publisher,
			time.Now,
		)
	if err != nil {
		return fmt.Errorf(
			"create classification worker: %w",
			err,
		)
	}

	handler, err :=
		application.NewClassificationCommandHandlerWithObserver(
			worker,
			classificationRetryDelays,
			config.classificationCommandDLQTopic,
			publisher,
			commandObserver,
		)
	if err != nil {
		return fmt.Errorf(
			"create classification command handler: %w",
			err,
		)
	}

	runner, err := kafkaadapter.NewRebalanceYieldConsumerRunner(
		kafkaClient,
		handler,
		rebalanceYield,
	)
	if err != nil {
		return fmt.Errorf(
			"create kafka consumer runner: %w",
			err,
		)
	}

	readiness := management.NewReadiness()

	managementHandler, err := management.NewHandler(
		readiness,
		registry,
	)
	if err != nil {
		return fmt.Errorf(
			"create management handler: %w",
			err,
		)
	}

	managementListener, err := net.Listen(
		"tcp",
		config.managementListenAddr,
	)
	if err != nil {
		return fmt.Errorf(
			"listen management HTTP on %q: %w",
			config.managementListenAddr,
			err,
		)
	}

	managementServer := &http.Server{
		Handler:           managementHandler,
		ReadHeaderTimeout: managementReadHeaderTimeout,
	}

	managementServeErrors := make(
		chan error,
		1,
	)

	go func() {
		serveErr := managementServer.Serve(
			managementListener,
		)

		if serveErr != nil &&
			!errors.Is(
				serveErr,
				http.ErrServerClosed,
			) {
			managementServeErrors <- serveErr
			cancelRun()
			return
		}

		managementServeErrors <- nil
	}()

	readiness.SetReady()

	logger.InfoContext(
		runContext,
		"service started",
		"operation", "startup",
		"kafka_client_id", config.kafkaClientID,
		"kafka_consumer_group", config.kafkaConsumerGroup,
		"command_topic", config.classificationCommandTopic,
		"result_topic", config.classificationResultTopic,
		"command_dlq_topic",
		config.classificationCommandDLQTopic,
		"model_bundle_version",
		config.modelBundleVersion,
		"management_listen_addr",
		config.managementListenAddr,
	)

	runnerErr := runner.Run(runContext)

	readiness.SetNotReady()
	cancelRun()

	shutdownContext, cancelShutdown :=
		context.WithTimeout(
			context.Background(),
			managementShutdownTimeout,
		)
	defer cancelShutdown()

	managementShutdownErr :=
		managementServer.Shutdown(
			shutdownContext,
		)

	managementServeErr := <-managementServeErrors

	if runnerErr != nil {
		return fmt.Errorf(
			"run classifier worker: %w",
			runnerErr,
		)
	}

	if managementServeErr != nil {
		return fmt.Errorf(
			"serve management HTTP: %w",
			managementServeErr,
		)
	}

	if managementShutdownErr != nil {
		return fmt.Errorf(
			"shutdown management HTTP: %w",
			managementShutdownErr,
		)
	}

	shutdownReason := "consumer_runner_stopped"
	if signalContext.Err() != nil {
		shutdownReason = "context_cancelled"
	}

	logger.Info(
		"service stopped",
		"operation", "shutdown",
		"shutdown_reason", shutdownReason,
	)

	return nil
}
