package pipelines

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"fun-with-channels/src/config"
	"fun-with-channels/src/models"
	"fun-with-channels/src/utilities"

	"github.com/gorilla/websocket"
)

type FinnhubDataPipelines struct {
	Config  *config.Config
	Channel chan models.FinnhubTradeData
	WSConn  *websocket.Conn
}

func NewFinnhubDataPipeline(config *config.Config) *FinnhubDataPipelines {
	return &FinnhubDataPipelines{
		Config:  config,
		Channel: make(chan models.FinnhubTradeData),
		WSConn:  nil,
	}
}

func (p *FinnhubDataPipelines) BeginFinnhubDataPipeline(db *sql.DB) {
	log.Printf("connecting to finnhub stock api %s", p.Config.FinnhubWebSocketURL)

	finnhubWebSocketURL := p.Config.FinnhubWebSocketURL + "?token=" + p.Config.FinnhubApiKey
	finnhubWebsocketConnection, _, err := websocket.DefaultDialer.Dial(finnhubWebSocketURL, nil)
	if err != nil {
		panic(finnhubWebsocketConnection)
	}
	p.WSConn = finnhubWebsocketConnection

	defer func(FinnhubWSConn *websocket.Conn) {
		err := FinnhubWSConn.Close()
		if err != nil {
			log.Println("error with finnhub websocket connection", err)
		}
	}(p.WSConn)

	for _, finnhubStockAPISymbol := range []string{models.BINANCEBTCUSDT, models.BINANCEETHUSDT, models.BINANCEADAUSDT} {
		log.Printf("subscribing to finnhub stock api symbols %s", finnhubStockAPISymbol)
		message, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": finnhubStockAPISymbol})

		err := p.WSConn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			panic(err)
		}
	}

	go p.listenForFinnhubStockAPIUpdates()

	go func() {
		for {
			time.Sleep(time.Minute)
			finnhubSimpleMovingAverageProcessor(db)
		}
	}()

	processFinnhubTradeData(p.Channel, db)
}

func (p *FinnhubDataPipelines) listenForFinnhubStockAPIUpdates() {
	var finnhubDataWrapper models.FinnhubDataWrapper
	log.Printf("receiving live finnhub stock api updates")
	for {

		err := p.WSConn.ReadJSON(&finnhubDataWrapper)
		if err != nil {
			panic(err)
		}

		switch finnhubDataWrapper.Type {
		case "trade":
			for _, finnhubTradeData := range finnhubDataWrapper.Data {
				p.Channel <- finnhubTradeData
			}
		}
	}
}

func processFinnhubTradeData(
	finnhubTradeDataChannel <-chan models.FinnhubTradeData, db *sql.DB,
) {
	for finnhubTradeData := range finnhubTradeDataChannel {
		utilities.InsertTradeData(db, finnhubTradeData, models.FinnhubTradeDataTable)
	}
}

func finnhubSimpleMovingAverageProcessor(db *sql.DB,
) {
	finnhubTradeDataFromDB, err := utilities.SelectTradeDataFromLastMinute(db, models.FinnhubTradeDataTable)
	if err != nil {
		log.Printf("error selecting finnhub trade data: %v", err)
	}
	utilities.CalculateSimpleMovingAverage(finnhubTradeDataFromDB, models.BINANCEBTCUSDT, db, models.FinnhubMovingAverageTable)
	utilities.CalculateSimpleMovingAverage(finnhubTradeDataFromDB, models.BINANCEETHUSDT, db, models.FinnhubMovingAverageTable)
	utilities.CalculateSimpleMovingAverage(finnhubTradeDataFromDB, models.BINANCEADAUSDT, db, models.FinnhubMovingAverageTable)
}
