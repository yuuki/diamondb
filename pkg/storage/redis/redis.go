package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	goredis "gopkg.in/redis.v5"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage/util"
)

const (
	oneYear time.Duration = time.Duration(24*360) * time.Hour
	oneWeek time.Duration = time.Duration(24*7) * time.Hour
	oneDay  time.Duration = time.Duration(24*1) * time.Hour

	redisBatchLimit = 50 // TODO need to tweak
)

// ReadWriter defines the interface for Redis reader and writer.
type ReadWriter interface {
	api() redisAPI
	Ping() error
	Fetch(string, time.Time, time.Time) (model.SeriesMap, error)
	batchGet(q *query) (model.SeriesMap, error)
	Get(string, string) (map[int64]float64, error)
	Len(string, string) (int64, error)
	Put(string, string, *model.Datapoint) error
	MPut(string, string, map[int64]float64) error
	Delete(string, string) error
}

type redisAPI interface {
	Ping() *goredis.StatusCmd
	Del(key ...string) *goredis.IntCmd
	HGetAll(key string) *goredis.StringStringMapCmd
	HSet(key, field string, value interface{}) *goredis.BoolCmd
	HMSet(key string, fields map[string]string) *goredis.StatusCmd
	HLen(key string) *goredis.IntCmd
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
	addrs, cluster := config.Config.RedisAddrs, config.Config.RedisCluster
	if len(addrs) > 1 || cluster {
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

// api returns the redis client.
func (r *Redis) api() redisAPI {
	return r.client
}

// Ping pings Redis server.
func (r *Redis) Ping() error {
	_, err := r.client.Ping().Result()
	if err != nil {
		return errors.Wrapf(err, "failed to ping redis")
	}
	return nil
}

// Fetch fetches datapoints by name from start until end.
func (r *Redis) Fetch(name string, start, end time.Time) (model.SeriesMap, error) {
	slot, step := selectTimeSlot(start, end)
	nameGroups := util.GroupNames(util.SplitName(name), redisBatchLimit)

	type result struct {
		value model.SeriesMap
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
	sm := make(model.SeriesMap, len(nameGroups))
	for i := 0; i < len(nameGroups); i++ {
		ret := <-c
		if ret.err != nil {
			return nil, errors.WithStack(ret.err)
		}
		sm.Merge(ret.value)
	}
	return sm, nil
}

func hGetAllToMap(name string, tsval map[string]string, q *query) (*model.SeriesPoint, error) {
	points := make(model.DataPoints, 0, len(tsval))
	for ts, val := range tsval {
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse timestamp %s", ts)
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse float value %s", val)
		}
		// Trim datapoints out of [start, end]
		if t < q.start.Unix() || q.end.Unix() < t {
			continue
		}
		points = append(points, model.NewDataPoint(t, v))
	}
	return model.NewSeriesPoint(name, points, q.step), nil
}

func (r *Redis) batchGet(q *query) (model.SeriesMap, error) {
	sm := make(model.SeriesMap, len(q.names))
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

// Get gets datapoints from redis by slot and series name.
func (r *Redis) Get(slot string, name string) (map[int64]float64, error) {
	key := slot + ":" + name
	tsval, err := r.client.HGetAll(key).Result()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to write (%s) from redis", key)
	}
	tv := make(map[int64]float64, len(tsval))
	for ts, val := range tsval {
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse timestamp %s", ts)
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse float value %s", val)
		}
		tv[t] = v
	}
	return tv, nil
}

// Len returns the length of datapoints by slot and name.
func (r *Redis) Len(slot string, name string) (int64, error) {
	key := slot + ":" + name
	n, err := r.client.HLen(key).Result()
	if err != nil {
		return -1, errors.Wrapf(err, "failed to get length (%s) from redis", key)
	}
	return n, nil
}

// Put puts the datapoint into redis.
func (r *Redis) Put(slot string, name string, p *model.Datapoint) error {
	key := slot + ":" + name
	err := r.client.HSet(key, fmt.Sprintf("%d", p.Timestamp), p.Value).Err()
	if err != nil {
		return errors.Wrapf(err, "failed to write (%s) from redis", key)
	}
	return nil
}

// MPut puts datapoints into redis.
func (r *Redis) MPut(slot string, name string, tv map[int64]float64) error {
	key := slot + ":" + name
	tsval := make(map[string]string, len(tv))
	for t, v := range tv {
		tsval[fmt.Sprintf("%d", t)] = fmt.Sprintf("%f", v)
	}
	if err := r.client.HMSet(key, tsval).Err(); err != nil {
		return errors.Wrapf(err, "failed to write (%s) from redis", key)
	}
	return nil
}

// Delete datapoints from redis.
func (r *Redis) Delete(slot string, name string) error {
	key := slot + ":" + name
	if err := r.client.Del(key).Err(); err != nil {
		return errors.Wrapf(err, "failed to write (%s) from redis", key)
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
