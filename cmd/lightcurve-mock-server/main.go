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
	"sync"
	"syscall"
	"time"

	kafkaadapter "github.com/ZChen470/variable-star-classification/internal/adapter/kafka"
	"github.com/ZChen470/variable-star-classification/internal/observability/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	lightCurveMockServiceName       = "lightcurve-mock-server"
	lightCurveMockReadHeaderTimeout = 5 * time.Second
	lightCurveMockShutdownTimeout   = 5 * time.Second
)

func main() {
	logger := logging.NewJSON(os.Stderr, lightCurveMockServiceName)
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("service failed", "operation", "run", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	config, err := loadLightCurveMockConfig(os.LookupEnv)
	if err != nil {
		return fmt.Errorf("load light curve mock config: %w", err)
	}

	dataset, err := loadLightCurveDataset(config.dataDir)
	if err != nil {
		return fmt.Errorf("load light curve mock dataset: %w", err)
	}

	listener, err := net.Listen("tcp", config.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", config.listenAddr, err)
	}
	defer listener.Close()

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.kafkaBrokers...),
		kgo.ClientID(config.kafkaClientID),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer kafkaClient.Close()

	messagePublisher := kafkaadapter.NewPublisher(kafkaClient)

	candidatePublisher, err := newCandidateEventPublisher(
		dataset,
		config.candidateTopic,
		config.candidateRatePerSecond,
		time.Now().UTC(),
		messagePublisher,
	)
	if err != nil {
		return fmt.Errorf("create CandidateEvent publisher: %w", err)
	}

	signalContext, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	runContext, cancelRun := context.WithCancel(signalContext)
	defer cancelRun()

	httpServer := &http.Server{
		Handler:           newLightCurveHTTPHandler(dataset),
		ReadHeaderTimeout: lightCurveMockReadHeaderTimeout,
	}

	componentErrors := make(chan error, 2)

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()

		err := httpServer.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		if err != nil {
			err = fmt.Errorf("serve LightCurve mock HTTP: %w", err)
		}

		componentErrors <- err
	}()

	go func() {
		defer waitGroup.Done()

		err := candidatePublisher.Run(runContext)
		if err != nil {
			err = fmt.Errorf("run CandidateEvent publisher: %w", err)
		}

		componentErrors <- err
	}()

	logger.InfoContext(
		runContext,
		"service started",
		"operation", "startup",
		"object_count", len(dataset.ObjectIDs()),
		"data_dir", config.dataDir,
		"listen_addr", config.listenAddr,
		"kafka_client_id", config.kafkaClientID,
		"candidate_topic", config.candidateTopic,
		"candidate_rate_per_second", config.candidateRatePerSecond,
	)

	var runErr error

	select {
	case <-signalContext.Done():
	case err := <-componentErrors:
		runErr = err
	}

	cancelRun()

	shutdownContext, cancelShutdown := context.WithTimeout(
		context.Background(),
		lightCurveMockShutdownTimeout,
	)
	defer cancelShutdown()

	shutdownErr := httpServer.Shutdown(shutdownContext)

	waitGroup.Wait()
	close(componentErrors)

	for err := range componentErrors {
		if runErr == nil && err != nil {
			runErr = err
		}
	}

	if shutdownErr != nil && runErr == nil {
		runErr = fmt.Errorf("shutdown LightCurve mock HTTP: %w", shutdownErr)
	}

	if runErr != nil {
		return runErr
	}

	shutdownReason := "component_stopped"
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
