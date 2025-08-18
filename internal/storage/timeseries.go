package storage

import (
	"sync"
	"time"

	"github.com/silasms/stream-data-pipeline/internal/analytics"
	"github.com/silasms/stream-data-pipeline/internal/domain"
)

type TimeSeriesStore struct {
	mu        sync.RWMutex
	series    map[string][]*domain.Event
	retention time.Duration
	capacity  int
}

func NewTimeSeriesStore(retention time.Duration, capacity int) *TimeSeriesStore {
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	if capacity <= 0 {
		capacity = 50000
	}
	return &TimeSeriesStore{
		series:    make(map[string][]*domain.Event),
		retention: retention,
		capacity:  capacity,
	}
}

func (s *TimeSeriesStore) Append(e *domain.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list := s.series[e.Metric]
	if len(list) >= s.capacity {
		evictCount := s.capacity / 10
		list = list[evictCount:]
	}

	s.series[e.Metric] = append(list, e)
}

func (s *TimeSeriesStore) Query(filter domain.QueryFilter) []*domain.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, exists := s.series[filter.Metric]
	if !exists {
		return []*domain.Event{}
	}

	results := make([]*domain.Event, 0)
	for _, e := range list {
		if !filter.From.IsZero() && e.Timestamp.Before(filter.From) {
			continue
		}
		if !filter.To.IsZero() && e.Timestamp.After(filter.To) {
			continue
		}
		if filter.TagKey != "" && e.Tags[filter.TagKey] != filter.TagValue {
			continue
		}
		results = append(results, e)
	}

	return results
}

func (s *TimeSeriesStore) Aggregate(metric string, from, to time.Time) *domain.WindowResult {
	events := s.Query(domain.QueryFilter{
		Metric: metric,
		From:   from,
		To:     to,
	})

	if len(events) == 0 {
		return nil
	}

	values := make([]float64, len(events))
	for i, e := range events {
		values[i] = e.Value
	}

	return analytics.CalculateWindowStats(metric, values, *events[0], *events[len(events)-1])
}

func (s *TimeSeriesStore) MetricsList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.series))
	for name := range s.series {
		names = append(names, name)
	}
	return names
}
