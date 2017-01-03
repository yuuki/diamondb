package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/yuuki/diamondb/lib/model"
	"github.com/yuuki/diamondb/lib/storage"
	redis "gopkg.in/redis.v5"
)

const (
	oneYear time.Duration = time.Duration(24*360) * time.Hour
	oneWeek time.Duration = time.Duration(24*7) * time.Hour
	oneDay  time.Duration = time.Duration(24*1) * time.Hour

	redisBatchLimit = 50 // TODO need to tweak
)

var (
	client *redis.Client
)

func FetchMetrics(name string, start, end time.Time) ([]*model.Metric, error) {
	slot, step := selectTimeSlot(start, end)
	nameGroups := storage.GroupNames(storage.SplitName(name), redisBatchLimit)
	c := make(chan interface{})
	for _, names := range nameGroups {
		concurrentBatchGet(slot, names, step, c)
	}
	var metrics []*model.Metric
	for i := 0; i < len(nameGroups); i++ {
		ret := <-c
		switch ret.(type) {
		case []*model.Metric:
			metrics = append(metrics, ret.([]*model.Metric)...)
		case error:
			return nil, errors.WithStack(ret.(error))
		}
	}
	return metrics, nil
}

func concurrentBatchGet(slot string, names []string, step int, c chan<- interface{}) {
	go func() {
		resp, err := batchGet(slot, names, step)
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

func hGetAllToMap(name string, tsval map[string]string, step int) (*model.Metric, error) {
	points := make([]*model.DataPoint, 0, len(tsval))
	for ts, val := range tsval {
		t, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to ParseInt timestamp %s", ts)
		}
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to ParseFloat value %s", v)
		}
		points = append(points, model.NewDataPoint(t, v))
	}
	return model.NewMetric(name, points, step), nil
}

func batchGet(slot string, names []string, step int) ([]*model.Metric, error) {
	metrics := make([]*model.Metric, 0, len(names))
	for _, name := range names {
		key := fmt.Sprintf("%s:%s", slot, name)
		tsval, err := client.HGetAll(key).Result()
		if err != nil {
			return nil, errors.Wrapf(err,
				"Failed to redis hgetall %s", strings.Join(names, ","),
			)
		}
		if len(tsval) < 1 {
			continue
		}
		metric, err := hGetAllToMap(name, tsval, step)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to hGetAllToMap %+v", tsval)
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
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
