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

type Redis struct {
	client *redis.Client
}

func NewRedis() *Redis {
	return &Redis{
		client: redis.NewClient(&redis.Options{
			Addr:     config.Config.RedisAddr,
			Password: config.Config.RedisPassword,
			DB:       config.Config.RedisDB,
		}),
	}
}

func (r *Redis) FetchSeriesMap(name string, start, end time.Time) (series.SeriesMap, error) {
	slot, step := selectTimeSlot(start, end)
	nameGroups := util.GroupNames(util.SplitName(name), redisBatchLimit)
	c := make(chan interface{})
	for _, names := range nameGroups {
		r.concurrentBatchGet(slot, names, step, c)
	}
	sm := make(series.SeriesMap, len(nameGroups))
	for i := 0; i < len(nameGroups); i++ {
		ret := <-c
		switch ret.(type) {
		case series.SeriesMap:
			sm.Merge(ret.(series.SeriesMap))
		case error:
			return nil, errors.WithStack(ret.(error))
		}
	}
	return sm, nil
}

func (r *Redis) concurrentBatchGet(slot string, names []string, step int, c chan<- interface{}) {
	go func() {
		resp, err := r.batchGet(slot, names, step)
		if err != nil {
			c <- errors.Wrapf(err,
				"Failed to redis batchGet %s %s %d",
				slot, strings.Join(names, ","), step,
			)
		} else {
			c <- resp
		}
	}()
}

func hGetAllToMap(name string, tsval map[string]string, step int) (*series.SeriesPoint, error) {
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
	return series.NewSeriesPoint(name, points, step), nil
}

func (r *Redis) batchGet(slot string, names []string, step int) (series.SeriesMap, error) {
	sm := make(series.SeriesMap, len(names))
	for _, name := range names {
		key := fmt.Sprintf("%s:%s", slot, name)
		tsval, err := r.client.HGetAll(key).Result()
		if err != nil {
			return nil, errors.Wrapf(err,
				"Failed to redis hgetall %s", strings.Join(names, ","),
			)
		}
		if len(tsval) < 1 {
			continue
		}
		sp, err := hGetAllToMap(name, tsval, step)
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
