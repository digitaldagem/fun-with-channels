package models

type FinnhubDataWrapper struct {
	Data []FinnhubTradeData `json:"data"`
	Type string             `json:"type"`
}

type FinnhubTradeData struct {
	LastPrice float64 `json:"p" db:"last_price"`
	Symbol    string  `json:"s" db:"symbol"`
	Timestamp int64   `json:"t" db:"time_stamp"`
	Volume    float64 `json:"v" db:"volume"`
}

type FinnhubMovingAverage struct {
	MovingAverage  float64 `db:"moving_average"`
	Symbol         string  `db:"symbol"`
	StartTimestamp int64   `db:"start_time_stamp"`
	EndTimestamp   int64   `db:"end_time_stamp"`
}

const (
	BINANCEBTCUSDT            = "BINANCE:BTCUSDT"
	BINANCEETHUSDT            = "BINANCE:ETHUSDT"
	BINANCEADAUSDT            = "BINANCE:ADAUSDT"
	FinnhubTradeDataTable     = "finnhub_trade_data"
	FinnhubMovingAverageTable = "finnhub_moving_averages"
)
