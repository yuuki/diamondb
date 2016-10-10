package config

import (
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

type DynamoDBConfig struct {
	Endpoint string
	Region   string
}

var (
	// Config represents dynamond's configuration file.
	Config = struct {
		Host     string
		Port     string
		Timezone string `toml:"time_zone"`

		DynamoDB *DynamoDBConfig
	}{}
)

// LoadConfig loads config file.
func Load(file string) error {
	if _, err := toml.DecodeFile(file, &Config); err != nil {
		return errors.Wrapf(err, "Failed to decode toml file %s", file)
	}

	return nil
}
