package storage

import (
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/yuuki/diamondb/pkg/config"
	"github.com/yuuki/diamondb/pkg/mathutil"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage/dynamodb"
	"github.com/yuuki/diamondb/pkg/storage/redis"
)

// ReadWriter defines the interface for data store reader and writer.
type ReadWriter interface {
	Ping() error
	Init() error
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
		return nil, err
	}
	return &Store{
		Redis:    redis.New(),
		DynamoDB: d,
	}, nil
}

// Ping pings each storage.
func (s *Store) Ping() error {
	eg := errgroup.Group{}
	eg.Go(func() error {
		return s.Redis.Ping()
	})
	eg.Go(func() error {
		return s.DynamoDB.Ping()
	})
	return eg.Wait()
}

// Init initializes the store object.
func (s *Store) Init() error {
	err := s.DynamoDB.CreateTable(&dynamodb.CreateTableParam{
		Name: config.Config.DynamoDBTableName,
		RCU:  config.Config.DynamoDBTableReadCapacityUnits,
		WCU:  config.Config.DynamoDBTableWriteCapacityUnits,
	})
	if err != nil {
		return err
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
		return nil, err
	}
	smD, err := fdynamodb.Get()
	if err != nil {
		return nil, err
	}

	ss := smR.MergePointsToSlice(smD)
	return ss, nil
}

var (
	retentions  = []string{"1m:1d", "5m:7d", "1h:30d", "1d:1y"}
	timeSlotMap = map[string]map[string]int{
		"1m": {
			"timestampStep":  60,
			"itemEpochStep":  60 * 60,
			"numberOfPoints": 5,
			"flushPoints":    5,
		},
		"5m": {
			"timestampStep":  60 * 5,
			"itemEpochStep":  60 * 60 * 24,
			"numberOfPoints": 12,
			"flushPoints":    12,
		},
		"1h": {
			"timestampStep":  60 * 60,
			"itemEpochStep":  60 * 60 * 24 * 7,
			"numberOfPoints": 24,
			"flushPoints":    24,
		},
		"1d": {
			"timestampStep":  60 * 60 * 24,
			"itemEpochStep":  60 * 60 * 24 * 365,
			"numberOfPoints": -1,
			"flushPoints":    1,
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
		slot := strings.SplitN(retentions[0], ":", 2)[0]
		if err := s.Redis.Put(slot, m.Name, p); err != nil {
			return err
		}

		for i, retention := range retentions {
			if i == (len(retentions) - 1) {
				break
			}

			parts := strings.SplitN(retention, ":", 2)
			slot, history := parts[0], parts[1]

			n, err := s.Redis.Len(slot, m.Name)
			if err != nil {
				return err
			}
			if n >= int64(timeSlotMap[slot]["numberOfPoints"]) {
				tv, err := s.Redis.Get(slot, m.Name)
				if err != nil {
					return err
				}
				nextSlot := strings.SplitN(retentions[i+1], ":", 2)[0]
				if err := s.rollup(nextSlot, m.Name, tv); err != nil {
					return err
				}
			}
			if n >= int64(timeSlotMap[slot]["flushPoints"]) {
				if err := s.flush(slot, history, m.Name); err != nil {
					return err
				}
			}
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
				return err
			}
		}
	}
	return nil
}

func (s *Store) flush(slot, history, name string) error {
	tv1, err := s.Redis.Get(slot, name)
	if err != nil {
		return err
	}
	for itemEpoch, tv2 := range groupByItemEpoch(slot, tv1) {
		if err := s.DynamoDB.Put(name, slot, history, itemEpoch, tv2); err != nil {
			return err
		}
	}
	if err := s.Redis.Delete(slot, name); err != nil {
		return err
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
