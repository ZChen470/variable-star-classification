package main

import (
	"context"
	"fmt"
	kafkaadapter "github.com/ZChen470/variable-star-classification/internal/adapter/kafka"
	postgresadapter "github.com/ZChen470/variable-star-classification/internal/adapter/postgres"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		log.Printf("classification result writer failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := loadClassificationResultWriterConfig(os.LookupEnv)
	if err != nil {
		return fmt.Errorf("load classification result writer config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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

	writer, err := application.NewClassificationResultWriterHandler(config.classificationResultTopic, repository)
	if err != nil {
		return fmt.Errorf("create classification result writer: %w", err)
	}

	handler, err := application.NewClassificationResultDLQHandler(writer, config.classificationResultDLQTopic, publisher)
	if err != nil {
		return fmt.Errorf("create classification result DLQ handler: %w", err)
	}

	runner, err := kafkaadapter.NewConsumerRunner(kafkaClient, handler)
	if err != nil {
		return fmt.Errorf("create Kafka consumer runner: %w", err)
	}

	log.Printf(
		"classification result writer started: client_id=%q consumer_group=%q result_topic=%q result_dlq_topic=%q",
		config.kafkaClientID,
		config.kafkaConsumerGroup,
		config.classificationResultTopic,
		config.classificationResultDLQTopic,
	)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run classification result writer: %w", err)
	}

	log.Printf("classification result writer stopped")

	return nil
}
