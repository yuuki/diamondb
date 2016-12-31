package config

import (
	"os"
	// "github.com/pkg/errors"
)

type config struct {
	Host string
	Port string

	Debug bool
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

	if os.Getenv("DIAMONDB_DEBUG") != "" {
		Config.Debug = true
	}

	return nil
}
