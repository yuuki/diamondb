package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	goredis "gopkg.in/redis.v5"

	"github.com/yuuki/diamondb/lib/config"
	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/util"
)

const (
	oneYear time.Duration = time.Duration(24*360) * time.Hour
	oneWeek time.Duration = time.Duration(24*7) * time.Hour
	oneDay  time.Duration = time.Duration(24*1) * time.Hour

	redisBatchLimit = 50 // TODO need to tweak
)

// ReadWriter defines the interface for Redis reader and writer.
type ReadWriter interface {
	Ping() error
	Fetch(string, time.Time, time.Time) (series.SeriesMap, error)
	Client() redisAPI
	batchGet(q *query) (series.SeriesMap, error)
	InsertDatapoint(string, string, *metric.Datapoint) error
}

type redisAPI interface {
	Ping() *goredis.StatusCmd
	HGetAll(key string) *goredis.StringStringMapCmd
	HSet(key, field string, value interface{}) *goredis.BoolCmd
	HMSet(key string, fields map[string]string) *goredis.StatusCmd
}

// Redis provides a redis client.
type Redis struct {
	client redisAPI
}

type query struct {
	names []string
	start time.Time
	end   time.Time
	slot  string
	step  int
	// context
}

var _ ReadWriter = &Redis{}

// New creates a Redis.
func New() *Redis {
	addrs := config.Config.RedisAddrs
	if len(addrs) > 1 {
		r := Redis{
			client: goredis.NewClusterClient(&goredis.ClusterOptions{
				Addrs:    config.Config.RedisAddrs,
				Password: config.Config.RedisPassword,
				PoolSize: config.Config.RedisPoolSize,
			}),
		}
		return &r
	} else if len(addrs) == 1 {
		r := Redis{
			client: goredis.NewClient(&goredis.Options{
				Addr:     config.Config.RedisAddrs[0],
				Password: config.Config.RedisPassword,
				DB:       config.Config.RedisDB,
				PoolSize: config.Config.RedisPoolSize,
			}),
		}
		return &r
	}
	return nil
}

// Client returns the redis client.
func (r *Redis) Client() redisAPI {
	return r.client
}

// Ping pings Redis server.
func (r *Redis) Ping() error {
	_, err := r.client.Ping().Result()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Fetch fetches datapoints by name from start until end.
func (r *Redis) Fetch(name string, start, end time.Time) (series.SeriesMap, error) {
	slot, step := selectTimeSlot(start, end)
	nameGroups := util.GroupNames(util.SplitName(name), redisBatchLimit)

	type result struct {
		value series.SeriesMap
		err   error
	}
	c := make(chan *result, len(nameGroups))
	for _, names := range nameGroups {
		q := &query{
			names: names,
			slot:  slot,
			start: start,
			end:   end,
			step:  step,
		}
		go func(q *query) {
			sm, err := r.batchGet(q)
			c <- &result{value: sm, err: err}
		}(q)
	}
	sm := make(series.SeriesMap, len(nameGroups))
	for i := 0; i < len(nameGroups); i++ {
		ret := <-c
		if ret.err != nil {
			return nil, errors.WithStack(ret.err)
		}
		sm.Merge(ret.value)
	}
	return sm, nil
}

func hGetAllToMap(name string, tsval map[string]string, q *query) (*series.SeriesPoint, error) {
	points := make(series.DataPoints, 0, len(tsval))
	for ts, val := range tsval {
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse timestamp %s", ts)
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse float value %s", v)
		}
		// Trim datapoints out of [start, end]
		if t < q.start.Unix() || q.end.Unix() < t {
			continue
		}
		points = append(points, series.NewDataPoint(t, v))
	}
	return series.NewSeriesPoint(name, points, q.step), nil
}

func (r *Redis) batchGet(q *query) (series.SeriesMap, error) {
	sm := make(series.SeriesMap, len(q.names))
	for _, name := range q.names {
		key := fmt.Sprintf("%s:%s", q.slot, name)
		tsval, err := r.client.HGetAll(key).Result()
		if err != nil {
			return nil, errors.Wrapf(err,
				"failed to hgetall api %s", strings.Join(q.names, ","),
			)
		}
		if len(tsval) < 1 {
			continue
		}
		sp, err := hGetAllToMap(name, tsval, q)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		sm[name] = sp
	}
	return sm, nil
}

func (r *Redis) InsertDatapoint(slot string, name string, p *metric.Datapoint) error {
	err := r.client.HSet(slot+":"+name, fmt.Sprintf("%d", p.Timestamp), p.Value).Err()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func selectTimeSlot(startTime, endTime time.Time) (string, int) {
	var (
		step int
		slot string
	)
	diffTime := endTime.Sub(startTime)
	if oneYear <= diffTime {
		slot = "1d"
		step = 60 * 60 * 24
	} else if oneWeek <= diffTime {
		slot = "1h"
		step = 60 * 60
	} else if oneDay <= diffTime {
		slot = "5m"
		step = 5 * 60
	} else {
		slot = "1m"
		step = 60
	}
	return slot, step
}
