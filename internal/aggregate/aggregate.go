package aggregate

import (
	"time"

	"market-analysis/internal/model"
)

type VWAPState struct {
	TotalPV     float64
	TotalVolume float64
}

type Aggregator struct {
	candles map[string]map[int64]*Candle // key: Symbol -> key: Minute Timestamp (Unix) -> Value: *Candle

	vwaps map[string]*VWAPState // key: Symbol -> Value: *VWAPState
}

// to create a clean instance
func NewAggregator() *Aggregator {
	return &Aggregator{
		candles: make(map[string]map[int64]*Candle),
		vwaps:   make(map[string]*VWAPState),
	}
}

// updating the analytics with a new trade
func (a *Aggregator) ProcessTrade(t model.Trade) {
	// finding the bucket (start of the minute)
	// eg: 10:05:32 converts to 10:05:00
	ts := t.Timestamp.Unix()
	bucketKey := ts - (ts % 60)
	bucketTime := time.Unix(bucketKey, 0).UTC()

	//initisalise symbol map if missing
	if _, ok := a.candles[t.Symbol]; !ok {
		a.candles[t.Symbol] = make(map[int64]*Candle)
	}

	//update OHLC Candle
	c, exists := a.candles[t.Symbol][bucketKey]
	if !exists {
		//then, new candle for this minute
		a.candles[t.Symbol][bucketKey] = NewCandle(t.Symbol, bucketTime, t.Price, t.Quantity, ts)
	} else {
		//update the existing candle
		c.Volume += t.Quantity

		//update High/Low
		if t.Price > c.High {
			c.High = t.Price
		}
		if t.Price < c.Low {
			c.Low = t.Price
		}

		//handle outoforder: Open
		//if this trade is earlier than the one that set the current Open,then update Open
		if ts < c.firstTs {
			c.Open = t.Price
			c.firstTs = ts
		}

		//handle Out-of-Order: Close
		//if this trade is later than the one that set the current Close,then update Close
		if ts >= c.lastTs {
			c.Close = t.Price
			c.lastTs = ts
		}
	}

	//applying formula
	if _, ok := a.vwaps[t.Symbol]; !ok {
		a.vwaps[t.Symbol] = &VWAPState{}
	}
	state := a.vwaps[t.Symbol]
	state.TotalPV += (t.Price * t.Quantity)
	state.TotalVolume += t.Quantity
}

// to return all candles for a symbol.
func (a *Aggregator) GetOHLC(symbol string) []*Candle {
	var results []*Candle
	if symbolMap, ok := a.candles[symbol]; ok {
		for _, c := range symbolMap {
			results = append(results, c)
		}
	}
	return results
}

// to return the current VWAP for a symbol.
func (a *Aggregator) GetVWAP(symbol string) float64 {
	state, ok := a.vwaps[symbol]
	if !ok || state.TotalVolume == 0 {
		return 0
	}
	return state.TotalPV / state.TotalVolume
}

// returns a list of all symbols currently tracked.
func (a *Aggregator) GetSymbols() []string {
	keys := make([]string, 0, len(a.candles))
	for k := range a.candles {
		keys = append(keys, k)
	}
	return keys
}
