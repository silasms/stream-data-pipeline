package domain

import (
	"errors"
	"time"
)

var (
	ErrEmptyMetricName  = errors.New("metric name cannot be empty")
	ErrInvalidTimestamp  = errors.New("event timestamp is invalid")
	ErrEmptyBatch        = errors.New("event batch cannot be empty")
	ErrMetricNotFound    = errors.New("metric not found")
)

type Event struct {
	ID        string            `json:"id"`
	Metric    string            `json:"metric"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

func (e *Event) Validate() error {
	if e.Metric == "" {
		return ErrEmptyMetricName
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	if e.Tags == nil {
		e.Tags = make(map[string]string)
	}
	return nil
}

type WindowResult struct {
	Metric      string    `json:"metric"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count       int64     `json:"count"`
	Sum         float64   `json:"sum"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	Mean        float64   `json:"mean"`
	Variance    float64   `json:"variance"`
	StdDev      float64   `json:"std_dev"`
	P50         float64   `json:"p50"`
	P90         float64   `json:"p90"`
	P99         float64   `json:"p99"`
}

type QueryFilter struct {
	Metric   string
	From     time.Time
	To       time.Time
	TagKey   string
	TagValue string
}
