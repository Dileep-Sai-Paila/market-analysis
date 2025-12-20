package main

import (
	"context"
	"errors"
	"log"
	"market-analysis/internal/aggregate"
	"market-analysis/internal/api"
	"market-analysis/internal/ingest"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM) //context should be cancelled with ctrl+c or SIGTERM
	defer stop()

	aggregator := aggregate.NewAggregator()

	log.Println("Starting data ingestion...")
	go func() {
		err := ingest.IngestFile("ticks.csv", aggregator)
		if err != nil {
			log.Printf("Error processing file: %v", err)
		}
	}()

	// settingup server and routes
	apiHandler := api.NewHandler(aggregator)

	mux := http.NewServeMux()
	// just a health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// registering the imp endpoints
	mux.HandleFunc("/symbols", apiHandler.HandleSymbols)
	mux.HandleFunc("/ohlc", apiHandler.HandleOHLC)
	mux.HandleFunc("/vwap", apiHandler.HandleVWAP)

	//mux := http.NewServeMux()

	// configs
	srv := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second, // timeouts to avoid resource leaks
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	//running on seperate goroutine so main-routine can block waiting for the signal
	go func() {
		log.Println("Server starting on port :8080...")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	//block until Signal received
	<-ctx.Done()
	log.Println("Shutdown signal received. Closing server...")

	//give the server 5 seconds to finish active requests
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited properly.")
}
