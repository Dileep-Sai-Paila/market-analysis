package main

import (
	"log"
	"market-analysis/internal/aggregate"
	"market-analysis/internal/api"
	"market-analysis/internal/ingest"
	"net/http"
	"time"
)

func main() {
	aggregator := aggregate.NewAggregator()

	log.Println("Starting data ingestion...")
	go func() {
		err := ingest.IngestFile("ticks.csv", aggregator)
		if err != nil {
			log.Printf("Error processing file: %v", err)
		}
	}()

	// 3. Setup Server and Routes
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
	}

	// atart the server
	log.Println("Server starting on port :8080...")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
