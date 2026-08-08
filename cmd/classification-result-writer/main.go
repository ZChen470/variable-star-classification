package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	kafkaadapter "github.com/ZChen470/variable-star-classification/internal/adapter/kafka"
	postgresadapter "github.com/ZChen470/variable-star-classification/internal/adapter/postgres"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const serviceName = "classification-result-writer"

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
	config, err := loadClassificationResultWriterConfig(os.LookupEnv)
	if err != nil {
		return fmt.Errorf("load classification result writer config: %w", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	pool, err := pgxpool.New(ctx, config.postgresDSN)
	if err != nil {
		return fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
		kgo.ConsumerGroup(config.kafkaConsumerGroup),
		kgo.ConsumeTopics(config.classificationResultTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer kafkaClient.CloseAllowingRebalance()

	publisher := kafkaadapter.NewPublisher(kafkaClient)
	repository := postgresadapter.NewClassificationRepository(pool)

	writer, err := application.NewClassificationResultWriterHandler(
		config.classificationResultTopic,
		repository,
	)
	if err != nil {
		return fmt.Errorf(
			"create classification result writer: %w",
			err,
		)
	}

	handler, err := application.NewClassificationResultDLQHandler(
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
		return fmt.Errorf("create Kafka consumer runner: %w", err)
	}

	logger.InfoContext(
		ctx,
		"service started",
		"operation", "startup",
		"kafka_client_id", config.kafkaClientID,
		"kafka_consumer_group", config.kafkaConsumerGroup,
		"result_topic", config.classificationResultTopic,
		"result_dlq_topic", config.classificationResultDLQTopic,
	)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf(
			"run classification result writer: %w",
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
