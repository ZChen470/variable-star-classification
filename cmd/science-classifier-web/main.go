package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	modelbundleadapter "github.com/ZChen470/variable-star-classification/internal/adapter/modelbundle"
	tritonadapter "github.com/ZChen470/variable-star-classification/internal/adapter/triton"
)

const (
	tritonHTTPTimeout = 30 * time.Second

	tritonMaxResponseBytes = int64(1 << 20)
)

func main() {
	if err := run(); err != nil {
		log.Printf(
			"science classifier web failed: %v",
			err,
		)

		os.Exit(1)
	}
}

func run() error {
	cfg, err :=
		loadConfig(
			os.LookupEnv,
		)
	if err != nil {
		return fmt.Errorf(
			"load config: %w",
			err,
		)
	}

	ctx, stop :=
		signal.NotifyContext(
			context.Background(),
			os.Interrupt,
			syscall.SIGTERM,
		)
	defer stop()

	servingResolver, err :=
		modelbundleadapter.
			NewFileServingBundleResolver(
				cfg.
					modelBundleManifestPath,
			)
	if err != nil {
		return fmt.Errorf(
			"load serving bundle manifest: %w",
			err,
		)
	}

	servingBundle, err :=
		servingResolver.
			ResolveServingBundle(
				ctx,
				cfg.modelBundleVersion,
			)
	if err != nil {
		return fmt.Errorf(
			"resolve serving bundle %q: %w",
			cfg.modelBundleVersion,
			err,
		)
	}

	tritonHTTPClient :=
		&http.Client{
			Timeout: tritonHTTPTimeout,
		}

	tritonClient, err :=
		tritonadapter.NewClient(
			cfg.tritonBaseURL,
			tritonHTTPClient,
			tritonMaxResponseBytes,
		)
	if err != nil {
		return fmt.Errorf(
			"create Triton client: %w",
			err,
		)
	}

	contractGate, err :=
		tritonadapter.
			NewModelContractGate(
				tritonClient,
			)
	if err != nil {
		return fmt.Errorf(
			"create Triton contract gate: %w",
			err,
		)
	}

	if err :=
		contractGate.Verify(
			ctx,
			servingBundle.Entrypoint,
		); err != nil {
		return fmt.Errorf(
			"verify Triton serving contract: %w",
			err,
		)
	}

	classifier, err :=
		tritonadapter.
			NewVariableStarClassifier(
				tritonClient,
				servingBundle.Entrypoint,
			)
	if err != nil {
		return fmt.Errorf(
			"create variable star classifier: %w",
			err,
		)
	}

	scienceServer, err :=
		newScienceServer(
			classifier,
			servingBundle,
		)
	if err != nil {
		return fmt.Errorf(
			"create science HTTP server: %w",
			err,
		)
	}

	httpServer :=
		&http.Server{
			Addr: cfg.listenAddr,

			Handler: scienceServer.routes(),

			ReadHeaderTimeout: 5 * time.Second,

			WriteTimeout: 35 * time.Second,

			IdleTimeout: 60 * time.Second,
		}

	errCh :=
		make(
			chan error,
			1,
		)

	go func() {
		log.Printf(
			"science classifier web started: http://%s model_bundle_version=%q model=%s:%s",
			cfg.listenAddr,
			servingBundle.
				ModelBundleVersion,
			servingBundle.
				Entrypoint.
				ModelName,
			servingBundle.
				Entrypoint.
				ModelVersion,
		)

		errCh <- httpServer.
			ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel :=
			context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
		defer cancel()

		if err :=
			httpServer.Shutdown(
				shutdownCtx,
			); err != nil {
			return fmt.Errorf(
				"shutdown HTTP server: %w",
				err,
			)
		}

		return nil

	case err := <-errCh:
		if errors.Is(
			err,
			http.ErrServerClosed,
		) {
			return nil
		}

		return fmt.Errorf(
			"serve HTTP: %w",
			err,
		)
	}
}
