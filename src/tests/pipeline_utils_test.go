package tests

import (
	"database/sql"
	"log"
	"testing"

	"fun-with-channels/src/models"
	"fun-with-channels/src/utilities"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestCalculateSimpleMovingAverage(t *testing.T) {
	// arrange
	tradeData := []models.FinnhubTradeData{
		{42587.16, models.BINANCEBTCUSDT, 1706438557159, 0.00246},
		{2292.7, models.BINANCEETHUSDT, 1706438557374, 0.1089},
		{42587.16, models.BINANCEADAUSDT, 1706438557654, 0.02351}}
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
	actual := utilities.CalculateSimpleMovingAverage(tradeData, models.BINANCEBTCUSDT, db, models.FinnhubMovingAverageTable)

	// assert
	assert.Equal(t, 42587.16, actual)
}

func TestSumPrices(t *testing.T) {
	// arrange
	prices := []float64{42587.16, 42587.16}

	// act
	actual := utilities.SumPrices(prices)

	// assert
	assert.Equal(t, 85174.32, actual)
}

func TestFindStartTimeStamp(t *testing.T) {
	// arrange
	timestamps := []int64{1706438557159, 1706438557654}

	// act
	actual := utilities.FindStartTimeStamp(timestamps)

	// assert
	assert.Equal(t, int64(1706438557159), actual)
}

func TestFindEndTimeStamp(t *testing.T) {
	// arrange
	timestamps := []int64{1706438557159, 1706438557654}

	// act
	actual := utilities.FindEndTimeStamp(timestamps)

	// assert
	assert.Equal(t, int64(1706438557654), actual)
}
