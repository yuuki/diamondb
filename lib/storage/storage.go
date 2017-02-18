package storage

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamodb"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

// Fetcher defines the interface for data store reader.
type Fetcher interface {
	Fetch(string, time.Time, time.Time) (series.SeriesSlice, error)
	Ping() error
}

type Writer interface {
	InsertMetric(*metric.Metric) error
}

// Store provides each data store client.
type Store struct {
	Redis    redis.Fetcher
	DynamoDB dynamodb.Fetcher
	// s3 client
}

// NewFetcher create a new Store wrapped by Fetcher.
func NewFetcher() Fetcher {
	d, err := dynamodb.NewDynamoDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Store{
		Redis:    redis.NewRedis(),
		DynamoDB: d,
	}, nil
}

type WriterStore struct {
	Redis    redis.Writer
	DynamoDB dynamo.Writer
	// dynamodb client
	// s3 client
}

func NewWriter() Writer {
	return &WriterStore{
		Redis:    redis.NewWriter(),
		DynamoDB: dynamo.NewDynamoDB(),
	}
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

func (s *WriterStore) InsertMetric(m *metric.Metric) error {
	for _, p := range m.Datapoints {
		if err := s.Redis.InsertDatapoint("1m", m.Name, p); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
