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
	"github.com/ZChen470/variable-star-classification/internal/observability/kafkabacklog"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/ZChen470/variable-star-classification/internal/observability/management"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	serviceName = "kafka-backlog-exporter"

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
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf(
			"load Kafka backlog exporter config: %w",
			err,
		)
	}

	registry := management.NewRegistry()

	backlogMetrics, err := kafkabacklog.New(registry)
	if err != nil {
		return fmt.Errorf(
			"create Kafka backlog metrics: %w",
			err,
		)
	}

	kafkaOptions := []kgo.Opt{
		kgo.SeedBrokers(cfg.kafkaBrokers...),
		kgo.ClientID(cfg.kafkaClientID),
	}

	if cfg.kafkaSASLUsername != "" {
		mechanism, err :=
			kafkaadapter.NewSCRAMSHA256Mechanism(
				cfg.kafkaSASLUsername,
				cfg.kafkaSASLPassword,
			)
		if err != nil {
			return fmt.Errorf(
				"create Kafka SCRAM-SHA-256 mechanism: %w",
				err,
			)
		}

		kafkaOptions = append(
			kafkaOptions,
			kgo.SASL(mechanism),
		)
	}

	kafkaClient, err := kgo.NewClient(kafkaOptions...)
	if err != nil {
		return fmt.Errorf(
			"create Kafka backlog client: %w",
			err,
		)
	}
	defer kafkaClient.Close()

	positionReader, err :=
		kafkabacklog.NewPositionReader(kafkaClient)
	if err != nil {
		return fmt.Errorf(
			"create Kafka position reader: %w",
			err,
		)
	}

	recordReader, err :=
		kafkabacklog.NewDirectRecordReader(kafkaClient)
	if err != nil {
		return fmt.Errorf(
			"create Kafka record reader: %w",
			err,
		)
	}

	brokerReader, err := kafkabacklog.NewBrokerReader(
		positionReader,
		recordReader,
	)
	if err != nil {
		return fmt.Errorf(
			"create Kafka broker reader: %w",
			err,
		)
	}

	source, err := kafkabacklog.NewSource(brokerReader)
	if err != nil {
		return fmt.Errorf(
			"create Kafka backlog source: %w",
			err,
		)
	}

	refresher, err := kafkabacklog.NewRefresher(
		source,
		backlogMetrics,
		cfg.kafkaConsumerGroup,
		cfg.kafkaTopic,
	)
	if err != nil {
		return fmt.Errorf(
			"create Kafka backlog refresher: %w",
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
		cfg.managementListenAddr,
	)
	if err != nil {
		return fmt.Errorf(
			"listen management HTTP on %q: %w",
			cfg.managementListenAddr,
			err,
		)
	}

	managementServer := &http.Server{
		Handler:           managementHandler,
		ReadHeaderTimeout: managementReadHeaderTimeout,
	}

	managementErr := make(chan error, 1)

	go func() {
		err := managementServer.Serve(
			managementListener,
		)
		if err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			managementErr <- err
			return
		}

		managementErr <- nil
	}()

	logger.Info(
		"service started",
		"operation", "startup",
		"management_listen_addr",
		cfg.managementListenAddr,
		"kafka_broker_count",
		len(cfg.kafkaBrokers),
		"kafka_consumer_group",
		cfg.kafkaConsumerGroup,
		"kafka_topic",
		cfg.kafkaTopic,
		"refresh_interval",
		cfg.refreshInterval.String(),
		"refresh_timeout",
		cfg.refreshTimeout.String(),
	)

	ticker := time.NewTicker(cfg.refreshInterval)
	defer ticker.Stop()

	firstSuccessfulSnapshot := false

	refresh := func() {
		refreshContext, cancel := context.WithTimeout(
			runContext,
			cfg.refreshTimeout,
		)
		defer cancel()

		startedAt := time.Now()

		err := refresher.Refresh(refreshContext)
		if err != nil {
			logger.Error(
				"Kafka backlog refresh failed",
				"operation", "refresh",
				"kafka_consumer_group",
				cfg.kafkaConsumerGroup,
				"kafka_topic",
				cfg.kafkaTopic,
				"duration_ms",
				time.Since(startedAt).Milliseconds(),
				"error", err,
			)
			return
		}

		logger.Info(
			"Kafka backlog refresh succeeded",
			"operation", "refresh",
			"kafka_consumer_group",
			cfg.kafkaConsumerGroup,
			"kafka_topic",
			cfg.kafkaTopic,
			"duration_ms",
			time.Since(startedAt).Milliseconds(),
		)

		if !firstSuccessfulSnapshot {
			firstSuccessfulSnapshot = true
			readiness.SetReady()

			logger.Info(
				"service ready",
				"operation", "readiness",
				"reason",
				"first_successful_snapshot",
			)
		}
	}

	// Refresh immediately. A failed first refresh leaves readiness false but
	// keeps the process alive so Kafka recovery can make the exporter ready
	// without a restart.
	refresh()

	var runErr error

	for {
		select {
		case <-runContext.Done():
			goto shutdown

		case err := <-managementErr:
			if err != nil {
				runErr = fmt.Errorf(
					"serve management HTTP: %w",
					err,
				)
				cancelRun()
			}
			goto shutdown

		case <-ticker.C:
			refresh()
		}
	}

shutdown:
	readiness.SetNotReady()

	shutdownContext, cancelShutdown :=
		context.WithTimeout(
			context.Background(),
			managementShutdownTimeout,
		)
	defer cancelShutdown()

	managementShutdownErr :=
		managementServer.Shutdown(shutdownContext)

	if managementShutdownErr != nil {
		if runErr != nil {
			return errors.Join(
				runErr,
				fmt.Errorf(
					"shutdown management HTTP: %w",
					managementShutdownErr,
				),
			)
		}

		return fmt.Errorf(
			"shutdown management HTTP: %w",
			managementShutdownErr,
		)
	}

	if runErr != nil {
		return runErr
	}

	shutdownReason := "context_cancelled"
	if signalContext.Err() == nil {
		shutdownReason = "management_server_stopped"
	}

	logger.Info(
		"service stopped",
		"operation", "shutdown",
		"shutdown_reason", shutdownReason,
	)

	return nil
}
