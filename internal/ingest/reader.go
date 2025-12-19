package ingest

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"market-analysis/internal/aggregate"
	"market-analysis/internal/model"
)

func IngestFile(filepath string, agg *aggregate.Aggregator) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	reader.FieldsPerRecord = -1 //to prevent the reader from erroring out immediately

	//reading 1st line and ignoring
	_, err = reader.Read()
	if err != nil {
		return err //edge case of file being empty
	}

	lineCount := 0

	// looping thru all lines
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			log.Printf("Skipping bad CSV line: %v", err) // csv format errors say quoting issues
			continue
		}

		if len(record) < 4 {
			continue
		}

		//parsing Timestam in my fav rfc3339 format
		ts, err := time.Parse(time.RFC3339Nano, record[0])
		if err != nil {
			log.Printf("Skipping invalid timestamp row %d: %s", lineCount, record[0]) //MALINFORMED_ROW
			continue
		}

		//parse price and quantity
		price, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			continue
		}

		qtyFloat, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			continue
		}

		trade := model.Trade{
			Timestamp: ts,
			Symbol:    record[1],
			Price:     price,
			Quantity:  qtyFloat,
		}

		//update Aggregator (Synchronously for now, just to check)
		agg.ProcessTrade(trade)
		lineCount++
	}

	log.Printf("Ingestion complete. Processed %d trades.", lineCount)
	return nil
}
