package analytics_test

import (
	"testing"
	"time"

	"github.com/silasms/stream-data-pipeline/internal/analytics"
	"github.com/silasms/stream-data-pipeline/internal/domain"
)

func TestCalculateWindowStats(t *testing.T) {
	values := []float64{10, 20, 30, 40, 50}
	now := time.Now()
	e1 := domain.Event{Timestamp: now.Add(-5 * time.Minute)}
	e2 := domain.Event{Timestamp: now}

	stats := analytics.CalculateWindowStats("latency_ms", values, e1, e2)
	if stats == nil {
		t.Fatalf("expected non-nil stats")
	}

	if stats.Count != 5 {
		t.Fatalf("expected count 5, got %d", stats.Count)
	}
	if stats.Mean != 30.0 {
		t.Fatalf("expected mean 30, got %f", stats.Mean)
	}
	if stats.Min != 10.0 || stats.Max != 50.0 {
		t.Fatalf("expected min 10 and max 50, got %f and %f", stats.Min, stats.Max)
	}
	if stats.P50 != 30.0 {
		t.Fatalf("expected P50 30, got %f", stats.P50)
	}
}

func TestExponentialMovingAverage(t *testing.T) {
	ema := analytics.NewEMA(0.5)
	ema.Add(100.0)
	val := ema.Add(200.0)

	if val != 150.0 {
		t.Fatalf("expected EMA 150, got %f", val)
	}
}
