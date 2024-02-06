package src

import (
	"database/sql"
	"log"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestCalculateSimpleMovingAverage(t *testing.T) {
	// arrange
	tradeData := []TradeData{
		{42587.16, "BINANCE:BTCUSDT", 1706438557159, 0.00246},
		{2292.7, "BINANCE:ETHUSDT", 1706438557374, 0.1089},
		{42587.16, "BINANCE:BTCUSDT", 1706438557654, 0.02351}}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			log.Println("error closing test database connection", err)
		}
	}(db)
	mock.ExpectBegin()
	mock.ExpectCommit()

	// act
	actual := calculateSimpleMovingAverage(tradeData, "BINANCE:BTCUSDT", db)

	// assert
	assert.Equal(t, 42587.16, actual)
}

func TestSumPrices(t *testing.T) {
	// arrange
	prices := []float64{42587.16, 42587.16}

	// act
	actual := sumPrices(prices)

	// assert
	assert.Equal(t, 85174.32, actual)
}

func TestFindStartTimeStamp(t *testing.T) {
	// arrange
	timestamps := []int64{1706438557159, 1706438557654}

	// act
	actual := findStartTimeStamp(timestamps)

	// assert
	assert.Equal(t, int64(1706438557159), actual)
}

func TestFindEndTimeStamp(t *testing.T) {
	// arrange
	timestamps := []int64{1706438557159, 1706438557654}

	// act
	actual := findEndTimeStamp(timestamps)

	// assert
	assert.Equal(t, int64(1706438557654), actual)
}
