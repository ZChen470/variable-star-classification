package main

import (
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestPercentile(t *testing.T) {
	values := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond}
	cases := []struct {
		name     string
		fraction float64
		want     time.Duration
	}{
		{"p50", 0.50, 20 * time.Millisecond},
		{"p95", 0.95, 40 * time.Millisecond},
		{"p99", 0.99, 40 * time.Millisecond},
		{"lower boundary", 0, 10 * time.Millisecond},
		{"upper boundary", 1, 40 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(values, tc.fraction); got != tc.want {
				t.Fatalf("percentile(%v, %v) = %v, want %v", values, tc.fraction, got, tc.want)
			}
		})
	}
	if got := percentile(nil, 0.95); got != 0 {
		t.Fatalf("percentile(nil, 0.95) = %v, want 0", got)
	}
}

func TestBenchmarkInput(t *testing.T) {
	first := benchmarkInput()
	second := benchmarkInput()
	if first.CoarseMode != application.CoarseModeComputeBootstrap {
		t.Fatalf("CoarseMode = %v, want COMPUTE_BOOTSTRAP", first.CoarseMode)
	}
	if len(first.TimeMJD) != 21 || len(first.Magnitude) != 21 || len(first.MagnitudeError) != 21 {
		t.Fatalf("unexpected input lengths: time=%d magnitude=%d error=%d", len(first.TimeMJD), len(first.Magnitude), len(first.MagnitudeError))
	}
	for i := 1; i < len(first.TimeMJD); i++ {
		if first.TimeMJD[i] <= first.TimeMJD[i-1] {
			t.Fatalf("observation time is not strictly increasing at index %d", i)
		}
	}
	first.TimeMJD[0] = 0
	first.Magnitude[0] = 0
	first.MagnitudeError[0] = 0
	if second.TimeMJD[0] != 60000.25 || second.Magnitude[0] != 15 || second.MagnitudeError[0] != 0.05 {
		t.Fatal("benchmarkInput returned shared or unexpected input data")
	}
}
