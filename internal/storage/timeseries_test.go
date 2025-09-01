package storage_test

import (
	"testing"
	"time"

	"github.com/silasms/stream-data-pipeline/internal/domain"
	"github.com/silasms/stream-data-pipeline/internal/storage"
)

func TestTimeSeriesStore_AppendAndQuery(t *testing.T) {
	store := storage.NewTimeSeriesStore(1*time.Hour, 100)
	now := time.Now()

	store.Append(&domain.Event{Metric: "cpu_usage", Value: 45.2, Timestamp: now.Add(-10 * time.Minute)})
	store.Append(&domain.Event{Metric: "cpu_usage", Value: 55.8, Timestamp: now.Add(-5 * time.Minute)})
	store.Append(&domain.Event{Metric: "cpu_usage", Value: 68.1, Timestamp: now})

	res := store.Query(domain.QueryFilter{Metric: "cpu_usage"})
	if len(res) != 3 {
		t.Fatalf("expected 3 events, got %d", len(res))
	}

	filtered := store.Query(domain.QueryFilter{
		Metric: "cpu_usage",
		From:   now.Add(-6 * time.Minute),
	})
	if len(filtered) != 2 {
		t.Fatalf("expected 2 events after filter, got %d", len(filtered))
	}

	stats := store.Aggregate("cpu_usage", now.Add(-15*time.Minute), now.Add(time.Minute))
	if stats == nil || stats.Count != 3 {
		t.Fatalf("expected aggregation over 3 events")
	}
}
