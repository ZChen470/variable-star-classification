package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestRebalanceYieldCancelsActiveRecord(t *testing.T) {
	t.Parallel()

	yield := NewRebalanceYield()
	baseline := yield.Generation()

	recordContext, release := yield.Bind(
		context.Background(),
		baseline,
	)
	defer release()

	if err := recordContext.Err(); err != nil {
		t.Fatalf("record context error before yield = %v", err)
	}

	yield.Request()

	if !errors.Is(recordContext.Err(), context.Canceled) {
		t.Fatalf(
			"record context error after yield = %v, want context.Canceled",
			recordContext.Err(),
		)
	}

	if !yield.RequestedSince(baseline) {
		t.Fatal("RequestedSince() = false, want true")
	}
}

func TestRebalanceYieldCancelsRecordWhenRequestArrivesBeforeBind(t *testing.T) {
	t.Parallel()

	yield := NewRebalanceYield()
	baseline := yield.Generation()

	yield.Request()

	recordContext, release := yield.Bind(
		context.Background(),
		baseline,
	)
	defer release()

	if !errors.Is(recordContext.Err(), context.Canceled) {
		t.Fatalf(
			"record context error = %v, want context.Canceled",
			recordContext.Err(),
		)
	}
}

func TestRebalanceYieldLeavesRecordActiveWithoutRequest(t *testing.T) {
	t.Parallel()

	yield := NewRebalanceYield()
	baseline := yield.Generation()

	recordContext, release := yield.Bind(
		context.Background(),
		baseline,
	)
	defer release()

	if err := recordContext.Err(); err != nil {
		t.Fatalf("record context error = %v, want nil", err)
	}

	if yield.RequestedSince(baseline) {
		t.Fatal("RequestedSince() = true, want false")
	}
}

func TestRebalanceYieldGenerationAdvancesForEachRequest(t *testing.T) {
	t.Parallel()

	yield := NewRebalanceYield()

	first := yield.Generation()
	yield.Request()
	second := yield.Generation()
	yield.Request()
	third := yield.Generation()

	if second != first+1 {
		t.Fatalf("second generation = %d, want %d", second, first+1)
	}
	if third != second+1 {
		t.Fatalf("third generation = %d, want %d", third, second+1)
	}
}
