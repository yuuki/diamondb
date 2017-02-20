package storage

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/mathutil"
	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamodb"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

// ReadWriter defines the interface for data store reader and writer.
type ReadWriter interface {
	Ping() error
	Fetch(string, time.Time, time.Time) (series.SeriesSlice, error)
	InsertMetric(*metric.Metric) error
}

// Store provides each data store client.
type Store struct {
	Redis    redis.ReadWriter
	DynamoDB dynamodb.ReadWriter
	// s3 client
}

// NewReadWriter create a new Store wrapped by ReadWriter.
func NewReadWriter() (ReadWriter, error) {
	d, err := dynamodb.NewDynamoDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Store{
		Redis:    redis.NewRedis(),
		DynamoDB: d,
	}, nil
}

// Ping pings each storage.
func (s *Store) Ping() error {
	rerr := s.Redis.Ping()
	derr := s.DynamoDB.Ping()
	if rerr != nil || derr != nil {
		var errMsg string
		if rerr != nil {
			errMsg += fmt.Sprintf("Redis connection error: %s \n", rerr)
		}
		if derr != nil {
			errMsg += fmt.Sprintf("DynamoDB connection error: %s ", derr)
		}
		return errors.New(errMsg)
	}
	return nil
}

type futureSeriesMap struct {
	result series.SeriesMap
	err    error
	done   chan struct{}
}

func newFutureSeriesMap() *futureSeriesMap {
	return &futureSeriesMap{
		done: make(chan struct{}, 1),
	}
}

func (f *futureSeriesMap) Get() (series.SeriesMap, error) {
	<-f.done
	return f.result, f.err
}

// Fetch fetches series from Redis, DynamoDB and S3.
// TODO S3
func (s *Store) Fetch(name string, start, end time.Time) (series.SeriesSlice, error) {
	fredis := newFutureSeriesMap()
	fdynamodb := newFutureSeriesMap()

	// Redis task
	go func(name string, start, end time.Time) {
		fredis.result, fredis.err = s.Redis.Fetch(name, start, end)
		fredis.done <- struct{}{}
	}(name, start, end)

	// DynamoDB task
	go func(name string, start, end time.Time) {
		fdynamodb.result, fdynamodb.err = s.DynamoDB.Fetch(name, start, end)
		fdynamodb.done <- struct{}{}
	}(name, start, end)

	smR, err := fredis.Get()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	smD, err := fdynamodb.Get()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ss := smR.MergePointsToSlice(smD)
	return ss, nil
}

var (
	timeSlotMap = map[string]map[string]int{
		"1m": {
			"timestampStep":  60,
			"itemEpochStep":  60 * 60,
			"tableEpochStep": 60 * 60 * 24,
		},
		"5m": {
			"timestampStep":  5 * 60,
			"itemEpochStep":  60 * 60 * 24,
			"tableEpochStep": 60 * 60 * 24,
		},
	}
)

func itemEpochFromTimestamp(slot string, timestamp int64) int64 {
	itemEpochStep := timeSlotMap[slot]["itemEpochStep"]
	return timestamp - timestamp%int64(itemEpochStep)
}

// InsertMetric inserts datapoints to Redis with rollup aggregation
// to DynamoDB if needed.
func (s *Store) InsertMetric(m *metric.Metric) error {
	for _, p := range m.Datapoints {
		tv, err := s.Redis.Get("1m", m.Name)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(tv) >= 5 {
			if err := s.rollup("5m", m.Name, tv); err != nil {
				return errors.WithStack(err)
			}
		}
		if err := s.Redis.Put("1m", m.Name, p); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *Store) rollup(slot string, name string, tvmap map[int64]float64) error {
	for itemEpoch, vals := range groupByItemEpoch(slot, tvmap) {
		p := &metric.Datapoint{
			Timestamp: itemEpoch,
			Value:     mathutil.AvgFloat64(vals),
		}
		if err := s.Redis.Put(slot, name, p); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func groupByItemEpoch(slot string, tv map[int64]float64) map[int64][]float64 {
	groups := map[int64][]float64{}
	for t, v := range tv {
		itemEpoch := itemEpochFromTimestamp(slot, t)
		if _, ok := groups[itemEpoch]; !ok {
			groups[itemEpoch] = []float64{}
		}
		groups[itemEpoch] = append(groups[itemEpoch], v)
	}
	return groups
}
