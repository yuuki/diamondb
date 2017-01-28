package storage

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamo"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

// Fetcher defines the interface for data store reader.
type Fetcher interface {
	Fetch(string, time.Time, time.Time) (series.SeriesSlice, error)
	Ping() error
}

// Store provides each data store client.
type Store struct {
	Redis    *redis.Redis
	DynamoDB *dynamo.DynamoDB
	// s3 client
}

// NewStore create a new Store wrapped by Fetcher.
func NewStore() Fetcher {
	return &Store{
		Redis:    redis.NewRedis(),
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
		return nil, errors.Wrapf(err, "redis.Fetch(%s,%d,%d)",
			name, start.Unix(), end.Unix(),
		)
	}
	smD, err := fdynamodb.Get()
	if err != nil {
		return nil, errors.Wrapf(err, "dynamodb.Fetch(%s,%d,%d)",
			name, start.Unix(), end.Unix(),
		)
	}

	sm := smR.MergePointsToSlice(smD)
	return sm, nil
}
