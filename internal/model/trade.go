package model

import "time"

// a single executed trade from the input stream.
type Trade struct {
	Timestamp time.Time
	Symbol    string
	Price     float64
	Quantity  float64 //float64 just to generalise than that of int or int64
}
