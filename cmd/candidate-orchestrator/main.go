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
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/observability/kafkametrics"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/ZChen470/variable-star-classification/internal/observability/management"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	serviceName                 = "candidate-orchestrator"
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
	config, err := loadCandidateOrchestratorConfig(
		os.LookupEnv,
	)
	if err != nil {
		return fmt.Errorf(
			"load candidate orchestrator config: %w",
			err,
		)
	}

	registry := management.NewRegistry()

	kafkaMetrics, err := kafkametrics.New(registry)
	if err != nil {
		return fmt.Errorf(
			"create Kafka metrics hook: %w",
			err,
		)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
		kgo.ConsumerGroup(config.kafkaConsumerGroup),
		kgo.ConsumeTopics(config.candidateTopic),
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
	defer client.CloseAllowingRebalance()

	policy, err := application.NewClassificationPolicyV1(
		config.modelBundleVersion,
	)
	if err != nil {
		return fmt.Errorf(
			"create classification policy: %w",
			err,
		)
	}

	publisher := kafkaadapter.NewPublisher(client)

	handler, err := application.NewCandidateHandler(
		config.candidateTopic,
		config.classificationCommandTopic,
		config.candidateDLQTopic,
		policy,
		publisher,
	)
	if err != nil {
		return fmt.Errorf(
			"create candidate handler: %w",
			err,
		)
	}

	runner, err := kafkaadapter.NewConsumerRunner(
		client,
		handler,
	)
	if err != nil {
		return fmt.Errorf(
			"create Kafka consumer runner: %w",
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
		"candidate_topic", config.candidateTopic,
		"command_topic", config.classificationCommandTopic,
		"candidate_dlq_topic", config.candidateDLQTopic,
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
			"run candidate orchestrator: %w",
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
