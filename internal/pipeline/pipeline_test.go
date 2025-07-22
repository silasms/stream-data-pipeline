package pipeline_test

import (
	"strings"
	"testing"
	"time"

	"github.com/silasms/stream-data-pipeline/internal/domain"
	"github.com/silasms/stream-data-pipeline/internal/pipeline"
)

func TestPipeline_ProcessingFlow(t *testing.T) {
	p := pipeline.NewPipeline(2, 50)

	p.AddTransform(func(e *domain.Event) (*domain.Event, error) {
		e.Metric = strings.ToUpper(e.Metric)
		return e, nil
	})

	p.AddFilter(func(e *domain.Event) bool {
		return e.Value > 0
	})

	p.Start()

	p.Ingest(&domain.Event{Metric: "bitrate", Value: 2500, Timestamp: time.Now()})
	p.Ingest(&domain.Event{Metric: "dropped_frames", Value: -5, Timestamp: time.Now()})

	select {
	case out := <-p.Output():
		if out.Metric != "BITRATE" {
			t.Fatalf("expected metric BITRATE, got %s", out.Metric)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for pipeline output")
	}

	p.Stop()
	proc, drop := p.Stats()
	if proc != 1 || drop != 1 {
		t.Fatalf("expected 1 processed and 1 dropped, got %d and %d", proc, drop)
	}
}

func BenchmarkPipelineThroughput(b *testing.B) {
	p := pipeline.NewPipeline(8, b.N)
	p.Start()
	defer p.Stop()

	go func() {
		for range p.Output() {
		}
	}()

	now := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Ingest(&domain.Event{
			Metric:    "qos.event",
			Value:     float64(i),
			Timestamp: now,
		})
	}
}
