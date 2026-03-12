package aggregate

import (
	"hash/fnv"
	"market-analysis/internal/model"
)

type ShardedAggregator struct {
	shards []*Aggregator
	n      uint32
}

func NewShardedAggregator(numShards int) *ShardedAggregator {
	shards := make([]*Aggregator, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewAggregator()
	}
	return &ShardedAggregator{
		shards: shards,
		n:      uint32(numShards),
	}
}

// hash based routing
func (s *ShardedAggregator) shardFor(symbol string) *Aggregator {
	h := fnv.New32a()
	h.Write([]byte(symbol))
	idx := h.Sum32() % s.n
	return s.shards[idx]
}

// write path
func (s *ShardedAggregator) ProcessTrade(t model.Trade) {
	shard := s.shardFor(t.Symbol)
	shard.ProcessTrade(t)
}

// read paths
func (s *ShardedAggregator) GetSymbols() []string {
	var result []string
	for _, shard := range s.shards {
		result = append(result, shard.GetSymbols()...)
	}
	return result
}

func (s *ShardedAggregator) GetOHLC(symbol string) []*Candle {
	return s.shardFor(symbol).GetOHLC(symbol)
}

func (s *ShardedAggregator) GetVWAP(symbol string) float64 {
	return s.shardFor(symbol).GetVWAP(symbol)
}
