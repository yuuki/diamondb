package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type config struct {
	ShutdownTimeout     time.Duration `json:"shutdown_timeout"`
	RedisAddrs          []string      `json:"redis_addrs"`
	RedisPassword       string        `json:"redis_password"`
	RedisDB             int           `json:"redis_db"`
	RedisPoolSize       int           `json:"redis_pool_size"`
	DynamoDBRegion      string        `json:"dynamodb_region"`
	DynamoDBTablePrefix string        `json:"dynamodb_table_prefix"`
	DynamoDBEndpoint    string        `json:"dynamodb_endpoint"`

	Debug bool `json:"debug"`
}

const (
	// DefaultPort is the default listening port.
	DefaultPort = "8000"
	// DefaultShutdownTimeout is the default timeout seconds for server shutdown.
	DefaultShutdownTimeout = 10 * time.Second
	// DefaultRedisAddr is the port to connect to redis-server process.
	DefaultRedisAddr = "localhost:6379"
	// DefaultRedisPassword is the password to connect to redis-server process.
	DefaultRedisPassword = ""
	// DefaultRedisDB is the redis db number.
	DefaultRedisDB = 0
	// DefaultRedisPoolSize is the redis pool size.
	DefaultRedisPoolSize = 50
	// DefaultDynamoDBRegion is the DynamoDB region.
	DefaultDynamoDBRegion = "ap-northeast-1"
	// DefaultDynamoDBTablePrefix is the prefix of DynamoDB table name.
	DefaultDynamoDBTablePrefix = "diamondb_datapoints"
)

// Config is set from the environment variables.
var Config = &config{}

// Load loads into Config from environment values.
func Load() error {
	timeout := os.Getenv("DIAMONDB_SHUTDOWN_TIMEOUT")
	if timeout == "" {
		Config.ShutdownTimeout = DefaultShutdownTimeout
	} else {
		v, err := strconv.Atoi(timeout)
		if err != nil {
			return errors.New("DIAMONDB_SHUTDOWN_TIMEOUT must be an integer")
		}
		Config.ShutdownTimeout = time.Duration(v) * time.Second
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
	redisPoolSize := os.Getenv("DIAMONDB_REDIS_POOL_SIZE")
	if redisPoolSize == "" {
		Config.RedisPoolSize = DefaultRedisPoolSize
	} else {
		v, err := strconv.Atoi(redisPoolSize)
		if err != nil {
			return errors.New("DIAMONDB_REDIS_POOL_SIZE must be an integer")
		}
		Config.RedisPoolSize = v
	}
	Config.DynamoDBRegion = os.Getenv("DIAMONDB_DYNAMODB_REGION")
	if Config.DynamoDBRegion == "" {
		Config.DynamoDBRegion = DefaultDynamoDBRegion
	}
	Config.DynamoDBTablePrefix = os.Getenv("DIAMONDB_DYNAMODB_TABLE_PREFIX")
	if Config.DynamoDBTablePrefix == "" {
		Config.DynamoDBTablePrefix = DefaultDynamoDBTablePrefix
	}
	if v := os.Getenv("DIAMONDB_DYNAMODB_ENDPOINT"); v != "" {
		Config.DynamoDBEndpoint = v
	}

	if os.Getenv("DIAMONDB_DEBUG") != "" {
		Config.Debug = true
	}

	return nil
}
