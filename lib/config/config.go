package config

import (
	"os"
	"strconv"

	"github.com/pkg/errors"
)

type config struct {
	Host           string
	Port           string
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	DynamoDBRegion string

	Debug bool
}

const (
	DefaultHost           = "localhost"
	DefaultPort           = "8000"
	DefaultRedisAddr      = "localhost:6379"
	DefaultRedisPassword  = ""
	DefaultRedisDB        = 0
	DefaultDynamoDBRegion = "ap-northeast-1"
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

	if os.Getenv("DIAMONDB_DEBUG") != "" {
		Config.Debug = true
	}

	return nil
}
