package aggregate

import (
	"market-analysis/internal/model"
	"testing"
	"time"
)

// Test 1: to verify basic OHLC and VWAP formulas
func TestAggregator_BasicMath(t *testing.T) {
	agg := NewAggregator()
	now := time.Now()

	//Trade 1: Price 100, Qty 10
	agg.ProcessTrade(model.Trade{Symbol: "TCS", Price: 100, Quantity: 10, Timestamp: now})
	//Trade 2: Price 200, Qty 10
	agg.ProcessTrade(model.Trade{Symbol: "TCS", Price: 200, Quantity: 10, Timestamp: now.Add(time.Second)})

	//check VWAP: (100*10 + 200*10) / 20 = 3000 / 20 = 150
	vwap := agg.GetVWAP("TCS")
	if vwap != 150 {
		t.Errorf("Expected VWAP 150, got %f", vwap)
	}

	//check High/Low
	candles := agg.GetOHLC("TCS")
	if len(candles) != 1 {
		t.Errorf("Expected 1 candle, got %d", len(candles))
	}
	c := candles[0]
	if c.High != 200 || c.Low != 100 {
		t.Errorf("OHLC incorrect. High: %f, Low: %f", c.High, c.Low)
	}
}

// Test 2: To verify Out-of-Order Logic (The Time Travel Test)
func TestAggregator_OutOfOrder(t *testing.T) {
	agg := NewAggregator()
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	//receive a trade at 10:00:05 first (Price 10)
	agg.ProcessTrade(model.Trade{Symbol: "INFY", Price: 10, Quantity: 1, Timestamp: baseTime.Add(5 * time.Second)})

	//receive a trade at 10:00:01 later (Price 5) -> This should become the OPEN
	agg.ProcessTrade(model.Trade{Symbol: "INFY", Price: 5, Quantity: 1, Timestamp: baseTime.Add(1 * time.Second)})

	candles := agg.GetOHLC("INFY")
	if candles[0].Open != 5 {
		t.Errorf("Expected Open to be 5 (earliest time), got %f", candles[0].Open)
	}
	if candles[0].Close != 10 {
		t.Errorf("Expected Close to be 10 (latest time), got %f", candles[0].Close)
	}
}

// Test 3: To verify Deduplication
func TestAggregator_Deduplication(t *testing.T) {
	agg := NewAggregator()
	now := time.Now()

	t1 := model.Trade{Symbol: "RELIANCE", Price: 1000, Quantity: 50, Timestamp: now}

	// Process the same trade twice
	agg.ProcessTrade(t1)
	agg.ProcessTrade(t1) // Should be ignored

	// Volume should be 50, not 100
	candles := agg.GetOHLC("RELIANCE")
	if candles[0].Volume != 50 {
		t.Errorf("Dedup failed. Expected volume 50, got %f", candles[0].Volume)
	}
}
