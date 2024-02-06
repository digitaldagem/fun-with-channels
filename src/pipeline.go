package src

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"fun-with-channels/src/config"
	"github.com/gorilla/websocket"
)

type Pipelines struct {
	Config  *config.Config
	Channel chan TradeData
	WSConn  *websocket.Conn
}

func NewPipeline(config *config.Config) *Pipelines {
	return &Pipelines{
		Config:  config,
		Channel: make(chan TradeData),
		WSConn:  nil,
	}
}

func (p *Pipelines) BeginDataPipeline(db *sql.DB) {
	log.Printf("connecting to stock api %s", p.Config.FinnhubWebSocketURL)

	finnhubWebSocketURL := p.Config.FinnhubWebSocketURL + "?token=" + p.Config.FinnhubApiKey
	websocketConnection, _, err := websocket.DefaultDialer.Dial(finnhubWebSocketURL, nil)
	if err != nil {
		panic(websocketConnection)
	}
	p.WSConn = websocketConnection

	defer func(FinnhubWSConn *websocket.Conn) {
		err := FinnhubWSConn.Close()
		if err != nil {
			log.Println("error with finnhub connection", err)
		}
	}(p.WSConn)

	for _, stockAPISymbol := range []string{BTCUSDT, ETHUSDT, ADAUSDT} {
		log.Printf("subscribing to stock api symbols %s", stockAPISymbol)
		message, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": stockAPISymbol})

		err := p.WSConn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			panic(err)
		}
	}

	go p.listenForStockAPIUpdates()

	go func() {
		for {
			time.Sleep(time.Minute)
			simpleMovingAverageProcessor(db)
		}
	}()

	processTradeData(p.Channel, db)
}

func (p *Pipelines) listenForStockAPIUpdates() {
	var dataWrapper DataWrapper
	log.Printf("receiving live stock api updates")
	for {

		err := p.WSConn.ReadJSON(&dataWrapper)
		if err != nil {
			panic(err)
		}

		switch dataWrapper.Type {
		case "trade":
			for _, tradeData := range dataWrapper.Data {
				p.Channel <- tradeData
			}
		}
	}
}

func processTradeData(
	tradeDataChannel <-chan TradeData, db *sql.DB,
) {
	for tradeData := range tradeDataChannel {
		InsertFinnhubTradeData(tradeData, db)
	}
}

func simpleMovingAverageProcessor(db *sql.DB,
) {
	tradeDataFromDB, err := SelectFinnhubTradeDataFromLastMinute(db)
	if err != nil {
		log.Printf("error selecting finnhub trade data: %v", err)
	}
	calculateSimpleMovingAverage(tradeDataFromDB, BTCUSDT, db)
	calculateSimpleMovingAverage(tradeDataFromDB, ETHUSDT, db)
	calculateSimpleMovingAverage(tradeDataFromDB, ADAUSDT, db)
}
