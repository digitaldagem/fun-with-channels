package main

import (
	"database/sql"
	"log"

	"fun-with-channels/src/config"
	"fun-with-channels/src/models"
	"fun-with-channels/src/pipelines"

	_ "github.com/lib/pq"
)

func main() {
	configuration, err := config.New()
	if err != nil {
		panic("error initializing config")
	}

	db, err := newDB(configuration)
	if err != nil {
		log.Fatal("error connecting to database: ", err)
	}
	defer func(db *sql.DB) {
		err = db.Close()
		if err != nil {
			log.Println("error closing database connection", err)
		}
	}(db)

	log.Println("Starting pipeline...")
	pipelines.NewFinnhubDataPipeline(configuration).BeginFinnhubDataPipeline(db)
}

func newDB(config *config.Config) (*sql.DB, error) {
	log.Println("Connecting to database...")

	db, err := sql.Open("postgres", config.DatabaseURL)
	if err != nil {
		panic(err)
	}

	statement := "CREATE TABLE IF NOT EXISTS " + models.FinnhubTradeDataTable + " (symbol VARCHAR(255), last_price float, time_stamp bigint, volume float);"
	_, err = db.Exec(statement)
	if err != nil {
		log.Fatal(err)
	}

	statement = "CREATE TABLE IF NOT EXISTS " + models.FinnhubMovingAverageTable + " (symbol VARCHAR(255), moving_average float, start_time_stamp bigint, end_time_stamp bigint);"
	_, err = db.Exec(statement)
	if err != nil {
		log.Fatal(err)
	}

	return db, nil
}
