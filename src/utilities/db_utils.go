package utilities

import (
	"database/sql"
	"log"

	"fun-with-channels/src/models"
)

func InsertTradeData(db *sql.DB, trade models.FinnhubTradeData, table string) {
	insertStatement := `INSERT INTO ` + table + ` (symbol, last_price, time_stamp, volume) VALUES ($1, $2, $3, $4)`
	transaction, err := db.Begin()
	if err != nil {
		log.Println("error beginning database insert transaction for "+table+" table", err)
	}

	defer func() {
		switch err {
		case nil:
			err = transaction.Commit()
		default:
			err = transaction.Rollback()
			if err != nil {
				log.Println("error rolling back database insert transaction for "+table+" table", err)
			}
		}
	}()
	_, err = transaction.Exec(insertStatement, trade.Symbol, trade.LastPrice, trade.Timestamp, trade.Volume)
	if err != nil {
		log.Println("error executing database insert transaction for "+table+" table", err)
	}
}

func SelectTradeDataFromLastMinute(db *sql.DB, table string) ([]models.FinnhubTradeData, error) {
	tradeData := make([]models.FinnhubTradeData, 0)
	selectStatement := `SELECT * FROM ` + table + ` WHERE to_timestamp(time_stamp / 1000) >= now() - INTERVAL '1 MINUTE';`
	transaction, err := db.Begin()
	if err != nil {
		log.Println("error beginning database select transaction for "+table+" table", err)
	}

	defer func() {
		switch err {
		case nil:
			err = transaction.Commit()
		default:
			err = transaction.Rollback()
			if err != nil {
				log.Println("error rolling back database select transaction for "+table+" table", err)
			}
		}
	}()
	rows, err := transaction.Query(selectStatement)
	if err != nil {
		log.Println("error running select query for "+table+" table", err)
	}
	for rows.Next() {
		var trade models.FinnhubTradeData
		err = rows.Scan(&trade.Symbol, &trade.LastPrice, &trade.Timestamp, &trade.Volume)
		if err != nil {
			log.Println("error scanning row from select query for "+table+" table", err)
		}
		tradeData = append(tradeData, trade)
	}
	return tradeData, nil
}

func InsertMovingAverage(db *sql.DB, movingAverage models.FinnhubMovingAverage, table string) {
	insertStatement := `INSERT INTO ` + table + ` (symbol, moving_average, start_time_stamp, end_time_stamp) VALUES ($1, $2, $3, $4)`
	transaction, err := db.Begin()
	if err != nil {
		log.Println("error beginning database insert transaction for "+table+" table", err)
	}

	defer func() {
		switch err {
		case nil:
			err = transaction.Commit()
		default:
			err = transaction.Rollback()
			if err != nil {
				log.Println("error rolling back database insert transaction for "+table+" table", err)
			}
		}
	}()
	_, err = transaction.Exec(insertStatement, movingAverage.Symbol, movingAverage.MovingAverage, movingAverage.StartTimestamp, movingAverage.EndTimestamp)
	if err != nil {
		log.Println("error executing database insert transaction for "+table+" table", err)
	}
	log.Printf("successfully inserted into "+table+" for %s: %f", movingAverage.Symbol, movingAverage.MovingAverage)
}
