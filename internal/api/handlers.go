package api

import (
	"encoding/json"
	"net/http"

	"market-analysis/internal/aggregate"
)

type Handler struct {
	agg *aggregate.Aggregator
}

func NewHandler(agg *aggregate.Aggregator) *Handler {
	return &Handler{agg: agg}
}

// GET /symbols
func (h *Handler) HandleSymbols(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	symbols := h.agg.GetSymbols()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(symbols)
}

// GET /ohlc?symbol=RELIANCE
func (h *Handler) HandleOHLC(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		http.Error(w, "Missing 'symbol' query parameter", http.StatusBadRequest)
		return
	}

	candles := h.agg.GetOHLC(symbol)

	w.Header().Set("Content-Type", "application/json")
	// If no data found, return empty list [] rather than null
	if candles == nil {
		w.Write([]byte("[]"))
		return
	}
	json.NewEncoder(w).Encode(candles)
}

// GET /vwap?symbol=TCS
func (h *Handler) HandleVWAP(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		http.Error(w, "Missing 'symbol' query parameter", http.StatusBadRequest)
		return
	}

	vwap := h.agg.GetVWAP(symbol)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"symbol": symbol,
		"vwap":   vwap,
	})
}
