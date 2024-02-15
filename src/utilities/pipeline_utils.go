package utilities

import (
	"database/sql"

	"fun-with-channels/src/models"
)

func CalculateSimpleMovingAverage(finnhubTradeData []models.FinnhubTradeData, symbol string, db *sql.DB, table string) float64 {
	timestamps := make([]int64, 0)
	prices := make([]float64, 0)
	for _, trade := range finnhubTradeData {
		if trade.Symbol == symbol {
			prices = append(prices, trade.LastPrice)
			timestamps = append(timestamps, trade.Timestamp)
		}
	}
	movingAverage := SumPrices(prices) / float64(len(prices))
	InsertMovingAverage(db,
		models.FinnhubMovingAverage{
			MovingAverage:  movingAverage,
			Symbol:         symbol,
			StartTimestamp: FindStartTimeStamp(timestamps),
			EndTimestamp:   FindEndTimeStamp(timestamps)},
		table)
	return movingAverage
}

func SumPrices(prices []float64) float64 {
	pricesSum := 0.0
	for _, price := range prices {
		pricesSum += price
	}
	return pricesSum
}

func FindStartTimeStamp(timestamps []int64) int64 {
	var startTimeStamp int64 = 9223372036854775807
	for _, timestamp := range timestamps {
		if timestamp < startTimeStamp {
			startTimeStamp = timestamp
		}
	}
	return startTimeStamp
}

func FindEndTimeStamp(timestamps []int64) int64 {
	var startTimeStamp int64 = 0
	for _, timestamp := range timestamps {
		if timestamp > startTimeStamp {
			startTimeStamp = timestamp
		}
	}
	return startTimeStamp
}
