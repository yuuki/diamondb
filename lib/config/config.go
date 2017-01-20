package config

import (
	"os"
	"strconv"

	"github.com/pkg/errors"
)

type config struct {
	Host                string
	Port                string
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	DynamoDBRegion      string
	DynamoDBTablePrefix string

	Debug bool
}

const (
	// DefaultPort is a default listening port
	DefaultPort = "8000"
	// DefaultRedisAddr is a port to connect to redis-server process
	DefaultRedisAddr = "localhost:6379"
	// DefaultRedisPassword is a password to connect to redis-server process
	DefaultRedisPassword = ""
	// DefaultRedisDB is a redis db number
	DefaultRedisDB = 0
	// DefaultDynamoDBRegion is a DynamoDB region
	DefaultDynamoDBRegion = "ap-northeast-1"
	// DefaultDynamoDBTablePrefix is a prefix of DynamoDB table name
	DefaultDynamoDBTablePrefix = "diamondb_datapoints"
)

// Config is set from the environment variables
var Config = &config{}

// Load loads into Config from environment values
func Load() error {
	Config.Port = os.Getenv("DIAMONDB_PORT")
	if Config.Port == "" {
		Config.Port = DefaultPort
	}
	Config.RedisAddr = os.Getenv("DIAMONDB_REDIS_ADDR")
	if Config.RedisAddr == "" {
		Config.RedisAddr = DefaultRedisAddr
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
		Config.DynamoDBTablePrefix = DefaultDynamoDBRegion
	}

	if os.Getenv("DIAMONDB_DEBUG") != "" {
		Config.Debug = true
	}

	return nil
}
