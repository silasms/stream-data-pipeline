package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/silasms/stream-data-pipeline/internal/domain"
	"github.com/silasms/stream-data-pipeline/internal/pipeline"
	"github.com/silasms/stream-data-pipeline/internal/storage"
)

type Server struct {
	pipeline *pipeline.Pipeline
	store    *storage.TimeSeriesStore
}

func NewServer(pipe *pipeline.Pipeline, store *storage.TimeSeriesStore) *Server {
	return &Server{
		pipeline: pipe,
		store:    store,
	}
}

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/events", s.IngestHandler)
	mux.HandleFunc("GET /api/v1/metrics", s.ListMetricsHandler)
	mux.HandleFunc("GET /api/v1/metrics/query", s.QueryHandler)
	mux.HandleFunc("GET /api/v1/metrics/aggregate", s.AggregateHandler)
	mux.HandleFunc("GET /health", s.HealthHandler)

	return mux
}

func (s *Server) IngestHandler(w http.ResponseWriter, r *http.Request) {
	var event domain.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, `{"error":"invalid_json"}`, http.StatusBadRequest)
		return
	}

	if err := event.Validate(); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	if !s.pipeline.Ingest(&event) {
		http.Error(w, `{"error":"pipeline_backpressure"}`, http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "ingested",
		"metric": event.Metric,
	})
}

func (s *Server) ListMetricsHandler(w http.ResponseWriter, r *http.Request) {
	metrics := s.store.MetricsList()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"metrics": metrics,
		"count":   len(metrics),
	})
}

func (s *Server) QueryHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		http.Error(w, `{"error":"missing_metric_param"}`, http.StatusBadRequest)
		return
	}

	events := s.store.Query(domain.QueryFilter{
		Metric:   metric,
		TagKey:   q.Get("tag_key"),
		TagValue: q.Get("tag_value"),
	})

	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"metric": metric,
		"count":  len(events),
		"data":   events,
	})
}

func (s *Server) AggregateHandler(w http.ResponseWriter, r *http.Request) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		http.Error(w, `{"error":"missing_metric_param"}`, http.StatusBadRequest)
		return
	}

	from := time.Now().Add(-1 * time.Hour)
	to := time.Now()

	stats := s.store.Aggregate(metric, from, to)
	if stats == nil {
		http.Error(w, `{"error":"no_data_in_range"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "UP"})
}
