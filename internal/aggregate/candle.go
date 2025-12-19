package aggregate

import "time"

//1-minute OHLC aggregation.
type Candle struct {
	Symbol string    `json:"symbol"`
	Start  time.Time `json:"start"`
	Open   float64   `json:"open"`
	High   float64   `json:"high"`
	Low    float64   `json:"low"`
	Close  float64   `json:"close"`
	Volume float64   `json:"volume"`

	firstTs int64 //to handle out of order trades
	lastTs  int64
}

//initializing candle with the first trade of that minute.
func NewCandle(symbol string, start time.Time, price, volume float64, ts int64) *Candle {
	return &Candle{
		Symbol:  symbol,
		Start:   start,
		Open:    price,
		High:    price,
		Low:     price,
		Close:   price,
		Volume:  volume,
		firstTs: ts,
		lastTs:  ts,
	}
}
