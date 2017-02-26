package storage

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/pkg/mathutil"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage/dynamodb"
	"github.com/yuuki/diamondb/pkg/storage/redis"
)

// ReadWriter defines the interface for data store reader and writer.
type ReadWriter interface {
	Ping() error
	Fetch(string, time.Time, time.Time) (model.SeriesSlice, error)
	InsertMetric(*model.Metric) error
}

// Store provides each data store client.
type Store struct {
	Redis    redis.ReadWriter
	DynamoDB dynamodb.ReadWriter
	// s3 client
}

var _ ReadWriter = &Store{}

// New create a new Store wrapped by ReadWriter.
func New() (*Store, error) {
	d, err := dynamodb.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Store{
		Redis:    redis.New(),
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
	result model.SeriesMap
	err    error
	done   chan struct{}
}

func newFutureSeriesMap() *futureSeriesMap {
	return &futureSeriesMap{
		done: make(chan struct{}, 1),
	}
}

func (f *futureSeriesMap) Get() (model.SeriesMap, error) {
	<-f.done
	return f.result, f.err
}

// Fetch fetches series from Redis, DynamoDB and S3.
// TODO S3
func (s *Store) Fetch(name string, start, end time.Time) (model.SeriesSlice, error) {
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
	timeSlots   = []string{"1m", "5m", "1h", "1d"}
	timeSlotMap = map[string]map[string]int{
		"1m": {
			"timestampStep":  60,
			"itemEpochStep":  60 * 60,
			"tableEpochStep": 60 * 60 * 24,
			"numberOfPoints": 5,
		},
		"5m": {
			"timestampStep":  60 * 5,
			"itemEpochStep":  60 * 60 * 24,
			"tableEpochStep": 60 * 60 * 24,
			"numberOfPoints": 12,
		},
		"1h": {
			"timestampStep":  60 * 60,
			"itemEpochStep":  60 * 60 * 24 * 7,
			"tableEpochStep": 60 * 60 * 24 * 7,
			"numberOfPoints": 24,
		},
		"1d": {
			"timestampStep":  60 * 60 * 24,
			"itemEpochStep":  60 * 60 * 24 * 365,
			"tableEpochStep": 60 * 60 * 24 * 365,
			"numberOfPoints": -1,
		},
	}
)

func itemEpochFromTimestamp(slot string, timestamp int64) int64 {
	itemEpochStep := timeSlotMap[slot]["itemEpochStep"]
	return timestamp - timestamp%int64(itemEpochStep)
}

func alignedTimestamp(slot string, timestamp int64) int64 {
	timestampStep := timeSlotMap[slot]["timestampStep"]
	return timestamp - timestamp%int64(timestampStep)
}

// InsertMetric inserts datapoints to Redis with rollup aggregation
// to DynamoDB if needed.
func (s *Store) InsertMetric(m *model.Metric) error {
	for _, p := range m.Datapoints {
		for i, slot := range timeSlots {
			if i == (len(timeSlots) - 1) {
				break
			}
			n, err := s.Redis.Len(slot, m.Name)
			if err != nil {
				return errors.WithStack(err)
			}
			if n >= int64(timeSlotMap[slot]["numberOfPoints"]) {
				tv, err := s.Redis.Get(slot, m.Name)
				if err != nil {
					return errors.WithStack(err)
				}
				if err := s.rollup(timeSlots[i+1], m.Name, tv); err != nil {
					return errors.WithStack(err)
				}
			}
		}
		if err := s.Redis.Put(timeSlots[0], m.Name, p); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *Store) rollup(slot string, name string, tvmap map[int64]float64) error {
	for _, tv := range groupByItemEpoch(slot, tvmap) {
		for t, vals := range groupByAlignedTimestamp(slot, tv) {
			p := &model.Datapoint{
				Timestamp: t,
				Value:     mathutil.AvgFloat64(vals),
			}
			if err := s.Redis.Put(slot, name, p); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func groupByAlignedTimestamp(slot string, tv map[int64]float64) map[int64][]float64 {
	groups := map[int64][]float64{}
	for t, v := range tv {
		aligned := alignedTimestamp(slot, t)
		if _, ok := groups[aligned]; !ok {
			groups[aligned] = []float64{}
		}
		groups[aligned] = append(groups[aligned], v)
	}
	return groups
}

func groupByItemEpoch(slot string, tv map[int64]float64) map[int64]map[int64]float64 {
	groups := map[int64]map[int64]float64{}
	for t, v := range tv {
		itemEpoch := itemEpochFromTimestamp(slot, t)
		if _, ok := groups[itemEpoch]; !ok {
			groups[itemEpoch] = map[int64]float64{}
		}
		groups[itemEpoch][t] = v
	}
	return groups
}
