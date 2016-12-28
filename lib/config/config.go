package config

import (
	"os"
	// "github.com/pkg/errors"
)

type config struct {
	Host string
	Port string
}

const (
	DefaultHost = "localhost"
	DefaultPort = "8000"
)

// Config is set from the environment variables
var Config = &config{}

func Load() error {
	Config.Host = os.Getenv("DIAMONDB_HOST")
	if Config.Host == "" {
		Config.Host = DefaultHost
	}
	Config.Port = os.Getenv("DIAMONDB_PORT")
	if Config.Port == "" {
		Config.Port = DefaultPort
	}

	return nil
}
