package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type config struct {
	ShutdownTimeout                 time.Duration  `json:"shutdown_timeout"`
	HTTPRenderTimeout               time.Duration  `json:"http_render_timeout"`
	TimeZoneName                    string         `json:"timezone"`
	TimeZone                        *time.Location `json:"-"`
	RedisCluster                    bool           `json:"redis_cluster"`
	RedisAddrs                      []string       `json:"redis_addrs"`
	RedisPassword                   string         `json:"-"`
	RedisDB                         int            `json:"redis_db"`
	RedisPoolSize                   int            `json:"redis_pool_size"`
	DynamoDBRegion                  string         `json:"dynamodb_region"`
	DynamoDBEndpoint                string         `json:"dynamodb_endpoint"`
	DynamoDBTableName               string         `json:"dynamodb_table_name"`
	DynamoDBTableReadCapacityUnits  int64          `json:"dynamodb_table_read_capacity_units"`
	DynamoDBTableWriteCapacityUnits int64          `json:"dynamodb_table_write_capacity_units"`
	DynamoDBTTL                     bool           `json:"dynamodb_ttl"`

	Debug bool `json:"debug"`
}

const (
	// DefaultPort is the default listening port.
	DefaultPort = "8000"
	// DefaultShutdownTimeout is the default timeout seconds for server shutdown.
	DefaultShutdownTimeout = 10 * time.Second
	// DefaultHTTPRenderTimeout is the default timeout seconds for /render.
	DefaultHTTPRenderTimeout = 30 * time.Second
	// DefaultTimeZone is the default timezone.
	DefaultTimeZone = "UTC"
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
	// DefaultDynamoDBTableName is the name of DynamoDB table.
	DefaultDynamoDBTableName = "diamondb.timeseries"
	// DefaultDynamoDBTableReadCapacityUnits is the name of DynamoDB table.
	DefaultDynamoDBTableReadCapacityUnits int64 = 5
	// DefaultDynamoDBTableWriteCapacityUnits is the name of DynamoDB table.
	DefaultDynamoDBTableWriteCapacityUnits int64 = 5
	// DefaultDynamoDBTTL is the flag of enabling DynamoDB TTL
	DefaultDynamoDBTTL = true
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
	renderTimeout := os.Getenv("DIAMONDB_HTTP_RENDER_TIMEOUT")
	if renderTimeout == "" {
		Config.HTTPRenderTimeout = DefaultHTTPRenderTimeout
	} else {
		v, err := strconv.ParseInt(renderTimeout, 10, 64)
		if err != nil {
			return errors.New("DIAMONDB_HTTP_RENDER_TIMEOUT must be an integer")
		}
		Config.HTTPRenderTimeout = time.Duration(v) * time.Second
	}

	Config.TimeZoneName = os.Getenv("DIAMONDB_TIMEZONE")
	if Config.TimeZoneName == "" {
		Config.TimeZone, _ = time.LoadLocation(DefaultTimeZone)
	} else {
		tz, err := time.LoadLocation(Config.TimeZoneName)
		if err != nil {
			return errors.New("DIAMONDB_TIMEZONE must be 'UTC', 'Local' or the name such as 'Asia/Tokyo'")
		}
		Config.TimeZone = tz
	}

	if v := os.Getenv("DIAMONDB_ENABLE_REDIS_CLUSTER"); v != "" {
		Config.RedisCluster = true
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
	Config.DynamoDBTableName = os.Getenv("DIAMONDB_DYNAMODB_TABLE_NAME")
	if Config.DynamoDBTableName == "" {
		Config.DynamoDBTableName = DefaultDynamoDBTableName
	}
	rcu := os.Getenv("DIAMONDB_DYNAMODB_TABLE_READ_CAPACITY_UNITS")
	if rcu == "" {
		Config.DynamoDBTableReadCapacityUnits = DefaultDynamoDBTableReadCapacityUnits
	} else {
		v, err := strconv.ParseInt(rcu, 10, 64)
		if err != nil {
			return errors.New("DIAMONDB_DYNAMODB_TABLE_READ_CAPACITY_UNITS must be an integer")
		}
		Config.DynamoDBTableReadCapacityUnits = v
	}
	wcu := os.Getenv("DIAMONDB_DYNAMODB_TABLE_WRITE_CAPACITY_UNITS")
	if wcu == "" {
		Config.DynamoDBTableWriteCapacityUnits = DefaultDynamoDBTableWriteCapacityUnits
	} else {
		v, err := strconv.ParseInt(wcu, 10, 64)
		if err != nil {
			return errors.New("DIAMONDB_DYNAMODB_TABLE_WRITE_CAPACITY_UNITS must be an integer")
		}
		Config.DynamoDBTableWriteCapacityUnits = v
	}
	if v := os.Getenv("DIAMONDB_DYNAMODB_ENDPOINT"); v != "" {
		Config.DynamoDBEndpoint = v
	}
	if v := os.Getenv("DIAMONDB_DYNAMODB_DISABLE_TTL"); v != "" {
		Config.DynamoDBTTL = false
	}

	if os.Getenv("DIAMONDB_DEBUG") != "" {
		Config.Debug = true
	}

	return nil
}
