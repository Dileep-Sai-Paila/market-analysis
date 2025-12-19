package main

import (
	"log"
	"net/http"
	"time"
)

func main() {
	mux := http.NewServeMux()

	// just a health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

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
