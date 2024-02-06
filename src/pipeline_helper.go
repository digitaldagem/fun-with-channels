package src

import (
	"database/sql"
	"log"
)

func InsertFinnhubTradeData(trade TradeData, db *sql.DB) {
	insertStatement := `INSERT INTO finnhub_trade_data (symbol, last_price, time_stamp, volume) VALUES ($1, $2, $3, $4)`
	transaction, err := db.Begin()
	if err != nil {
		log.Println("error beginning database insert transaction for trade data table", err)
	}

	defer func() {
		switch err {
		case nil:
			err = transaction.Commit()
		default:
			err = transaction.Rollback()
			if err != nil {
				log.Println("error rolling back database insert transaction for trade data table", err)
			}
		}
	}()
	_, err = transaction.Exec(insertStatement, trade.Symbol, trade.LastPrice, trade.Timestamp, trade.Volume)
	if err != nil {
		log.Println("error executing database insert transaction for trade data table", err)
	}
	log.Printf("successfully inserted Finnhub trade data for %s: %f", trade.Symbol, trade.LastPrice)
}

func SelectFinnhubTradeDataFromLastMinute(db *sql.DB) ([]TradeData, error) {
	tradeData := make([]TradeData, 0)
	selectStatement := `SELECT * FROM finnhub_trade_data WHERE to_timestamp(time_stamp / 1000) >= now() - INTERVAL '1 MINUTE';`
	transaction, err := db.Begin()
	if err != nil {
		log.Println("error beginning database select transaction for trade data table", err)
	}

	defer func() {
		switch err {
		case nil:
			err = transaction.Commit()
		default:
			err = transaction.Rollback()
			if err != nil {
				log.Println("error rolling back database select transaction for trade data table", err)
			}
		}
	}()
	rows, err := transaction.Query(selectStatement)
	if err != nil {
		log.Println("error running select query for trade data table", err)
	}
	for rows.Next() {
		var trade TradeData
		err = rows.Scan(&trade.Symbol, &trade.LastPrice, &trade.Timestamp, &trade.Volume)
		if err != nil {
			log.Println("error scanning row from select query for trade data table", err)
		}
		tradeData = append(tradeData, trade)
	}
	return tradeData, nil
}

func InsertFinnhubMovingAverage(movingAverage MovingAverage, db *sql.DB) {
	insertStatement := `INSERT INTO finnhub_moving_averages (symbol, moving_average, start_time_stamp, end_time_stamp) VALUES ($1, $2, $3, $4)`
	transaction, err := db.Begin()
	if err != nil {
		log.Println("error beginning database insert transaction for moving average table", err)
	}

	defer func() {
		switch err {
		case nil:
			err = transaction.Commit()
		default:
			err = transaction.Rollback()
			if err != nil {
				log.Println("error rolling back database insert transaction for moving average table", err)
			}
		}
	}()
	_, err = transaction.Exec(insertStatement, movingAverage.Symbol, movingAverage.MovingAverage, movingAverage.StartTimestamp, movingAverage.EndTimestamp)
	if err != nil {
		log.Println("error executing database insert transaction for moving average table", err)
	}
	log.Printf("successfully inserted Finnhub simple moving average for %s: %f", movingAverage.Symbol, movingAverage.MovingAverage)
}

func calculateSimpleMovingAverage(tradeData []TradeData, symbol string, db *sql.DB) float64 {
	timestamps := make([]int64, 0)
	prices := make([]float64, 0)
	for _, trade := range tradeData {
		if trade.Symbol == symbol {
			prices = append(prices, trade.LastPrice)
			timestamps = append(timestamps, trade.Timestamp)
		}
	}
	movingAverage := sumPrices(prices) / float64(len(prices))
	InsertFinnhubMovingAverage(
		MovingAverage{
			movingAverage,
			symbol,
			findStartTimeStamp(timestamps),
			findEndTimeStamp(timestamps)},
		db)
	return movingAverage
}

func sumPrices(prices []float64) float64 {
	pricesSum := 0.0
	for _, price := range prices {
		pricesSum += price
	}
	return pricesSum
}

func findStartTimeStamp(timestamps []int64) int64 {
	var startTimeStamp int64 = 9223372036854775807
	for _, timestamp := range timestamps {
		if timestamp < startTimeStamp {
			startTimeStamp = timestamp
		}
	}
	return startTimeStamp
}

func findEndTimeStamp(timestamps []int64) int64 {
	var startTimeStamp int64 = 0
	for _, timestamp := range timestamps {
		if timestamp > startTimeStamp {
			startTimeStamp = timestamp
		}
	}
	return startTimeStamp
}
