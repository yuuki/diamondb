package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type config struct {
	Host                string   `json:"host"`
	Port                string   `json:"port"`
	RedisAddrs          []string `json:"redis_addrs"`
	RedisPassword       string   `json:"redis_password"`
	RedisDB             int      `json:"redis_db"`
	DynamoDBRegion      string   `json:"dynamodb_region"`
	DynamoDBTablePrefix string   `json:"dynamodb_table_prefix"`

	Debug bool `json:"debug"`
}

const (
	// DefaultPort is the default listening port.
	DefaultPort = "8000"
	// DefaultRedisAddr is the port to connect to redis-server process.
	DefaultRedisAddr = "localhost:6379"
	// DefaultRedisPassword is the password to connect to redis-server process.
	DefaultRedisPassword = ""
	// DefaultRedisDB is the redis db number.
	DefaultRedisDB = 0
	// DefaultDynamoDBRegion is the DynamoDB region.
	DefaultDynamoDBRegion = "ap-northeast-1"
	// DefaultDynamoDBTablePrefix is the prefix of DynamoDB table name.
	DefaultDynamoDBTablePrefix = "diamondb_datapoints"
)

// Config is set from the environment variables.
var Config = &config{}

// Load loads into Config from environment values.
func Load() error {
	Config.Port = os.Getenv("DIAMONDB_PORT")
	if Config.Port == "" {
		Config.Port = DefaultPort
	}
	Config.RedisAddrs = strings.Split(os.Getenv("DIAMONDB_REDIS_ADDRS"), ",")
	if len(Config.RedisAddrs) == 0 {
		Config.RedisAddrs = []string{DefaultRedisAddr}
	}
	Config.RedisPassword = os.Getenv("DIAMONDB_REDIS_PASSWORD")
	if Config.RedisPassword == "" {
		Config.RedisPassword = DefaultRedisPassword
	}
	redisdb := os.Getenv("DIAMONDB_REDIS_DB")
	if redisdb == "" {
		Config.RedisDB = DefaultRedisDB
	} else {
		v, err := strconv.Atoi(redisdb)
		if err != nil {
			return errors.New("DIAMONDB_REDIS_DB must be an integer")
		}
		Config.RedisDB = v
	}
	Config.DynamoDBRegion = os.Getenv("DIAMONDB_DYNAMODB_REGION")
	if Config.DynamoDBRegion == "" {
		Config.DynamoDBRegion = DefaultDynamoDBRegion
	}
	Config.DynamoDBTablePrefix = os.Getenv("DIAMONDB_DYNAMODB_TABLE_PREFIX")
	if Config.DynamoDBTablePrefix == "" {
		Config.DynamoDBTablePrefix = DefaultDynamoDBTablePrefix
	}

	if os.Getenv("DIAMONDB_DEBUG") != "" {
		Config.Debug = true
	}

	return nil
}
