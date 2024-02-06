package config

import (
	"os"
)

type Config struct {
	DatabaseURL         string
	FinnhubApiKey       string
	FinnhubWebSocketURL string
}

func New() (*Config, error) {
	dbURL, found := os.LookupEnv("DATABASE_URL")
	if !found {
		panic("DATABASE_URL not found")
	}

	FinnhubApiKey, found := os.LookupEnv("FINNHUB_API_KEY")
	if !found {
		panic("FINNHUB_API_KEY not found")
	}

	finnhubWSURL, found := os.LookupEnv("FINNHUB_WEBSOCKET_URL")
	if !found {
		panic("FINNHUB_WEBSOCKET_URL not found")
	}

	return &Config{
		DatabaseURL:         dbURL,
		FinnhubApiKey:       FinnhubApiKey,
		FinnhubWebSocketURL: finnhubWSURL,
	}, nil
}
