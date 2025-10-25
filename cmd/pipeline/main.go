package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	adapterHTTP "github.com/silasms/stream-data-pipeline/internal/http"
	"github.com/silasms/stream-data-pipeline/internal/pipeline"
	"github.com/silasms/stream-data-pipeline/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Starting Stream Data Pipeline Engine")

	pipe := pipeline.NewPipeline(8, 50000)
	store := storage.NewTimeSeriesStore(24*time.Hour, 100000)

	pipe.Start()

	go func() {
		for event := range pipe.Output() {
			store.Append(event)
		}
	}()

	server := adapterHTTP.NewServer(pipe, store)
	httpServer := &http.Server{
		Addr:         ":8080",
		Handler:      server.Router(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("HTTP Telemetry Listener running on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-stopChan
	logger.Info("Shutting down stream pipeline...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(shutdownCtx)
	pipe.Stop()
	logger.Info("Stream Data Pipeline stopped cleanly")
}
