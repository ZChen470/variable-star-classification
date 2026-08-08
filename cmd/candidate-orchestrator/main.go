package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	kafkaadapter "github.com/ZChen470/variable-star-classification/internal/adapter/kafka"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

const serviceName = "candidate-orchestrator"

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

	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
		kgo.ConsumerGroup(config.kafkaConsumerGroup),
		kgo.ConsumeTopics(config.candidateTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
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
		return fmt.Errorf("create candidate handler: %w", err)
	}

	runner, err := kafkaadapter.NewConsumerRunner(
		client,
		handler,
	)
	if err != nil {
		return fmt.Errorf("create Kafka consumer runner: %w", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	logger.InfoContext(
		ctx,
		"service started",
		"operation", "startup",
		"kafka_client_id", config.kafkaClientID,
		"kafka_consumer_group", config.kafkaConsumerGroup,
		"candidate_topic", config.candidateTopic,
		"command_topic", config.classificationCommandTopic,
		"candidate_dlq_topic", config.candidateDLQTopic,
	)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run candidate orchestrator: %w", err)
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
