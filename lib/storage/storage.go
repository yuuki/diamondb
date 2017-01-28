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

// Fetch fetches series from Redis, DynamoDB and S3.
func (s *Store) Fetch(name string, start, end time.Time) (series.SeriesSlice, error) {
	type item struct {
		result series.SeriesMap
		err    error
	}

	redisCh := make(chan item, 1)
	dynamodbCh := make(chan item, 1)

	// Redis task
	go func(name string, start, end time.Time) {
		sm, err := s.Redis.Fetch(name, start, end)
		redisCh <- item{result: sm, err: err}
	}(name, start, end)

	// DynamoDB task
	go func(name string, start, end time.Time) {
		sm, err := s.DynamoDB.Fetch(name, start, end)
		dynamodbCh <- item{result: sm, err: err}
	}(name, start, end)

	var (
		smR, smD series.SeriesMap
	)
	select {
	case rit := <-redisCh:
		if rit.err != nil {
			return nil, errors.Wrapf(rit.err, "redis.Fetch(%s,%d,%d)",
				name, start.Unix(), end.Unix(),
			)
		}
		smR = rit.result
	case dit := <-dynamodbCh:
		if dit.err != nil {
			return nil, errors.Wrapf(dit.err, "dynamodb.Fetch(%s,%d,%d)",
				name, start.Unix(), end.Unix(),
			)
		}
		smD = dit.result
		// TODO timeout
	}
	// TODO S3

	sm := smR.MergePointsToSlice(smD)
	return sm, nil
}
