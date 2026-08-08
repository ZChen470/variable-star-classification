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
	postgresadapter "github.com/ZChen470/variable-star-classification/internal/adapter/postgres"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/observability/kafkametrics"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/ZChen470/variable-star-classification/internal/observability/management"
	"github.com/ZChen470/variable-star-classification/internal/observability/postgresmetrics"
	"github.com/ZChen470/variable-star-classification/internal/observability/resultmetrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	serviceName                 = "classification-result-writer"
	managementReadHeaderTimeout = 5 * time.Second
	managementShutdownTimeout   = 5 * time.Second
)

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
	config, err := loadClassificationResultWriterConfig(
		os.LookupEnv,
	)
	if err != nil {
		return fmt.Errorf(
			"load classification result writer config: %w",
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

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
		kgo.ConsumerGroup(config.kafkaConsumerGroup),
		kgo.ConsumeTopics(config.classificationResultTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.WithHooks(kafkaMetrics),
	)
	if err != nil {
		return fmt.Errorf(
			"create Kafka client: %w",
			err,
		)
	}
	defer kafkaClient.CloseAllowingRebalance()

	publisher := kafkaadapter.NewPublisher(kafkaClient)

	repository :=
		postgresadapter.NewClassificationRepository(pool)

	observedRepository, err := resultmetrics.NewSaver(
		registry,
		repository,
	)
	if err != nil {
		return fmt.Errorf(
			"create result persistence metrics: %w",
			err,
		)
	}

	writer, err :=
		application.NewClassificationResultWriterHandler(
			config.classificationResultTopic,
			observedRepository,
		)
	if err != nil {
		return fmt.Errorf(
			"create classification result writer: %w",
			err,
		)
	}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			writer,
			config.classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		return fmt.Errorf(
			"create classification result DLQ handler: %w",
			err,
		)
	}

	runner, err := kafkaadapter.NewConsumerRunner(
		kafkaClient,
		handler,
	)
	if err != nil {
		return fmt.Errorf(
			"create Kafka consumer runner: %w",
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

	// 到这里已经完成：
	// - PostgreSQL startup Ping
	// - Kafka client
	// - Result Writer / Result DLQ / ConsumerRunner
	// - management listener bind
	readiness.SetReady()

	logger.InfoContext(
		runContext,
		"service started",
		"operation", "startup",
		"kafka_client_id", config.kafkaClientID,
		"kafka_consumer_group", config.kafkaConsumerGroup,
		"result_topic", config.classificationResultTopic,
		"result_dlq_topic",
		config.classificationResultDLQTopic,
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
			"run classification result writer: %w",
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
