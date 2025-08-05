package storage

import (
	"sync"
	"time"

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
