package http_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	adapterHTTP "github.com/silasms/stream-data-pipeline/internal/http"
	"github.com/silasms/stream-data-pipeline/internal/pipeline"
	"github.com/silasms/stream-data-pipeline/internal/storage"
)

func TestServer_IngestAndQuery(t *testing.T) {
	pipe := pipeline.NewPipeline(2, 50)
	pipe.Start()
	defer pipe.Stop()

	store := storage.NewTimeSeriesStore(time.Hour, 100)

	go func() {
		for e := range pipe.Output() {
			store.Append(e)
		}
	}()

	srv := adapterHTTP.NewServer(pipe, store)
	router := srv.Router()

	payload := `{"metric":"video.bitrate","value":4500.0,"tags":{"cdn":"fastly_edge"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewBufferString(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", w.Code)
	}

	time.Sleep(50 * time.Millisecond)

	qReq := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/query?metric=video.bitrate", nil)
	qW := httptest.NewRecorder()
	router.ServeHTTP(qW, qReq)

	if qW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", qW.Code)
	}
}
