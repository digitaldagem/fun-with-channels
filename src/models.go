package src

type DataWrapper struct {
	Data []TradeData `json:"data"`
	Type string      `json:"type"`
}

type TradeData struct {
	LastPrice float64 `json:"p" db:"last_price"`
	Symbol    string  `json:"s" db:"symbol"`
	Timestamp int64   `json:"t" db:"time_stamp"`
	Volume    float64 `json:"v" db:"volume"`
}

type MovingAverage struct {
	MovingAverage  float64 `db:"moving_average"`
	Symbol         string  `db:"symbol"`
	StartTimestamp int64   `db:"start_time_stamp"`
	EndTimestamp   int64   `db:"end_time_stamp"`
}

const (
	BTCUSDT = "BINANCE:BTCUSDT"
	ETHUSDT = "BINANCE:ETHUSDT"
	ADAUSDT = "BINANCE:ADAUSDT"
)
