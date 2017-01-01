package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/yuuki/diamondb/lib/model"
	redis "gopkg.in/redis.v5"
)

var (
	client *redis.Client
)

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
				"Failed to redis hmget %s", strings.Join(names, ","),
			)
		}
		metric, err := hGetAllToMap(name, tsval, step)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to batchGetResultToMap %+v", tsval)
		}
		metrics = append(metrics, metric)
	}
	return metrics, nil
}
