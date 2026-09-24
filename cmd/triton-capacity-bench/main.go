package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/adapter/modelbundle"
	"github.com/ZChen470/variable-star-classification/internal/adapter/triton"
	"github.com/ZChen470/variable-star-classification/internal/application"
)

const modelBundleVersion = "variable-classifier-2026-07-003"

type result struct {
	duration       time.Duration
	err            error
	plannedToStart time.Duration
}

func main() {
	baseURL := flag.String("url", "", "Direct Triton Pod HTTP URL")
	manifest := flag.String("manifest", "models/bundles/model-bundle-manifest-v2.yaml", "Serving bundle manifest")
	concurrency := flag.Int("concurrency", 0, "Number of concurrent requests (1..32)")
	requests := flag.Int("requests", 0, "Total requests in unpaced mode (1..5000)")
	rate := flag.Int("rate", 0, "Target request dispatch rate in requests/s (paced mode)")
	duration := flag.Duration("duration", 0, "Request dispatch duration (paced mode)")
	timeout := flag.Duration("timeout", 10*time.Second, "Per-request timeout")
	flag.Parse()

	if *baseURL == "" || *concurrency < 1 || *concurrency > 32 || *timeout <= 0 {
		log.Fatal("require -url, -concurrency 1..32, and positive -timeout")
	}

	plan, requestCount, paced, err := resolveWorkload(*requests, *rate, *duration)
	if err != nil {
		log.Fatal(err)
	}

	resolver, err := modelbundle.NewFileServingBundleResolver(*manifest)
	if err != nil {
		log.Fatal(err)
	}

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelSetup()

	bundle, err := resolver.ResolveServingBundle(setupCtx, modelBundleVersion)
	if err != nil {
		log.Fatal(err)
	}

	httpClient := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			MaxConnsPerHost:     64,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	defer httpClient.CloseIdleConnections()

	client, err := triton.NewClient(*baseURL, httpClient, 4<<20)
	if err != nil {
		log.Fatal(err)
	}

	gate, err := triton.NewModelContractGate(client)
	if err != nil {
		log.Fatal(err)
	}
	if err := gate.Verify(setupCtx, bundle.Entrypoint); err != nil {
		log.Fatalf("Triton contract verification failed: %v", err)
	}

	classifier, err := triton.NewVariableStarClassifier(client, bundle.Entrypoint)
	if err != nil {
		log.Fatal(err)
	}

	outcome := runWorkload(
		context.Background(),
		plan,
		requestCount,
		paced,
		*concurrency,
		*timeout,
		func(ctx context.Context, input application.ClassificationInput) error {
			_, callErr := classifier.Classify(ctx, input)
			return callErr
		},
	)
	results := outcome.results
	submitted := outcome.submitted
	dispatchErr := outcome.dispatchErr
	dispatchElapsed := outcome.dispatchElapsed
	elapsed := outcome.elapsed

	latencies := make([]time.Duration, 0, submitted)
	plannedToStart := make([]time.Duration, 0, submitted)
	failures := 0
	for _, item := range results[:submitted] {
		if paced {
			plannedToStart = append(plannedToStart, item.plannedToStart)
		}
		if item.err != nil {
			failures++
			continue
		}
		latencies = append(latencies, item.duration)
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	successes := len(latencies)
	if paced {
		fmt.Printf("target=%s mode=COMPUTE_BOOTSTRAP epochs=21 concurrency=%d requests=%d rate=%d/s duration=%s\n",
			*baseURL, *concurrency, requestCount, plan.rate, plan.duration)
		fmt.Printf("dispatch_submitted=%d planned=%d dispatch_elapsed=%s actual_dispatch_rate=%.2f_requests_per_second\n",
			submitted, plan.requests, dispatchElapsed,
			float64(submitted)/dispatchElapsed.Seconds())
	} else {
		fmt.Printf("target=%s mode=COMPUTE_BOOTSTRAP epochs=21 concurrency=%d requests=%d\n",
			*baseURL, *concurrency, requestCount)
	}
	fmt.Printf("elapsed=%s successes=%d failures=%d throughput=%.2f_successes_per_second\n",
		elapsed, successes, failures, float64(successes)/elapsed.Seconds())

	if successes > 0 {
		fmt.Printf("latency_p50=%s latency_p95=%s latency_p99=%s\n",
			percentile(latencies, 0.50),
			percentile(latencies, 0.95),
			percentile(latencies, 0.99))
	}
	if paced && len(plannedToStart) > 0 {
		sort.Slice(plannedToStart, func(i, j int) bool {
			return plannedToStart[i] < plannedToStart[j]
		})
		fmt.Printf("planned_to_classify_start_p50=%s p95=%s p99=%s max=%s\n",
			percentile(plannedToStart, 0.50),
			percentile(plannedToStart, 0.95),
			percentile(plannedToStart, 0.99),
			plannedToStart[len(plannedToStart)-1])
	}

	if outcome.firstErr != nil {
		log.Fatalf("rate benchmark stopped after request failure: %v; submitted=%d/%d",
			outcome.firstErr, submitted, requestCount)
	}

	if dispatchErr != nil {
		log.Fatalf("rate benchmark dispatch failed after %d/%d submissions: %v", submitted, requestCount, dispatchErr)
	}

	if failures > 0 {
		log.Fatal("benchmark contained failed requests; do not treat throughput as a passing capacity result")
	}
}

func benchmarkInput() application.ClassificationInput {
	const epochs = 21

	timeMJD := make([]float64, epochs)
	magnitude := make([]float32, epochs)
	magnitudeError := make([]float32, epochs)

	for index := 0; index < epochs; index++ {
		timeMJD[index] = 60000.25 + float64(index)*0.75
		magnitude[index] = float32(15.0 + 0.3*math.Sin(float64(index)*0.35))
		magnitudeError[index] = 0.05 + float32(index%3)*0.001
	}

	return application.ClassificationInput{
		TimeMJD:        timeMJD,
		Magnitude:      magnitude,
		MagnitudeError: magnitudeError,
		CoarseMode:     application.CoarseModeComputeBootstrap,
	}
}

func percentile(sorted []time.Duration, fraction float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(fraction*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
