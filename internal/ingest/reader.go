package ingest

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"market-analysis/internal/aggregate"
	"market-analysis/internal/model"
)

// stream the csv file thru a worker pool
func IngestFile(ctx context.Context, filepath string, agg *aggregate.Aggregator) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 //to prevent the reader from erroring out immediately

	//ignoring line 1
	if _, err := reader.Read(); err != nil {
		return err
	}

	linesCh := make(chan []string, 100)
	tradesCh := make(chan model.Trade, 100)

	numWorkers := 4 // 4 coz for my system, i felt like it a conservative default for encoding/csv (single threaded IO bound operation)
	var wg sync.WaitGroup

	//start workers (fan-out)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(linesCh, tradesCh)
		}()
	}

	//start Aggregator consumer (fan-in)
	//runs in background collectimg results
	doneCh := make(chan struct{})
	go func() {
		count := 0
		for t := range tradesCh {
			agg.ProcessTrade(t) // Only 1 goroutine touches the map here
			count++
		}
		log.Printf("Ingestion complete. Processed %d trades.", count)
		close(doneCh) // signaling that aggregation is finished and no more to be done
	}()

	//Producer: read file and feed workers
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Ingestion cancelled by user.")
				return //if it reaches here, then stop reading immediately
			default:
				// Continue normal execution
			}
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Skipping bad line: %v", err)
				continue
			}
			linesCh <- record //send work to pool
		}
		close(linesCh) //telling workers no more data is coming
	}()

	wg.Wait()       //waiting for workers to finish parsing
	close(tradesCh) //then we close the trades channel so the Aggregator knows to stop

	//finally, wait for the Aggregator to finish the last update
	<-doneCh
	return nil
}

// converts raw CSV lines into Trade objects
func worker(lines <-chan []string, trades chan<- model.Trade) {
	for record := range lines {
		//basic Validation
		if len(record) < 4 {
			continue
		}

		//parsing Timestamp
		ts, err := time.Parse(time.RFC3339Nano, record[0])
		if err != nil {
			continue //skip "MALFORMED_ROW" silently or log if debugging
		}

		//parsing Price
		price, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			continue
		}

		//parsing Quantity
		qty, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			continue
		}

		//send valid trade to aggregator
		trades <- model.Trade{
			Timestamp: ts,
			Symbol:    record[1],
			Price:     price,
			Quantity:  qty,
		}
	}
}
