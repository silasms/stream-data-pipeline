package domain_test

import (
	"testing"

	"github.com/silasms/stream-data-pipeline/internal/domain"
)

func TestEvent_Validate(t *testing.T) {
	e := &domain.Event{
		Metric: "video.buffer_ratio",
		Value:  0.024,
	}

	if err := e.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if e.Timestamp.IsZero() {
		t.Fatalf("expected timestamp to default to now")
	}

	invalid := &domain.Event{
		Metric: "",
		Value:  10.0,
	}
	if err := invalid.Validate(); err != domain.ErrEmptyMetricName {
		t.Fatalf("expected ErrEmptyMetricName, got %v", err)
	}
}
