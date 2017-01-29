package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/yuuki/diamondb/lib/config"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/util"
	redis "gopkg.in/redis.v5"
)

const (
	oneYear time.Duration = time.Duration(24*360) * time.Hour
	oneWeek time.Duration = time.Duration(24*7) * time.Hour
	oneDay  time.Duration = time.Duration(24*1) * time.Hour

	redisBatchLimit = 50 // TODO need to tweak
)

// Redis provides a redis client.
type Redis struct {
	client *redis.Client
}

type query struct {
	names []string
	start time.Time
	end   time.Time
	slot  string
	step  int
	// context
}

// NewRedis creates a Redis.
func NewRedis() *Redis {
	return &Redis{
		client: redis.NewClient(&redis.Options{
			Addr:     config.Config.RedisAddr,
			Password: config.Config.RedisPassword,
			DB:       config.Config.RedisDB,
		}),
	}
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
			return nil, errors.Wrapf(err, "Failed to ParseInt timestamp %s", ts)
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to ParseFloat value %s", v)
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
				"Failed to redis hgetall %s", strings.Join(q.names, ","),
			)
		}
		if len(tsval) < 1 {
			continue
		}
		sp, err := hGetAllToMap(name, tsval, q)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to hGetAllToMap %+v", tsval)
		}
		sm[name] = sp
	}
	return sm, nil
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
