package storage

import (
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamo"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

// Fetcher defines the interface for data store reader.
type Fetcher interface {
	Fetch(string, time.Time, time.Time) (series.SeriesSlice, error)
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

// FetchSeriesSlice fetches series from Redis, DynamoDB and S3.
func (s *Store) Fetch(name string, start, end time.Time) (series.SeriesSlice, error) {
	sm1, err := s.Redis.Fetch(name, start, end)
	if err != nil {
		return nil, errors.Wrapf(err,
			"Failed to redis.FetchMetrics %s %d %d",
			name, start.Unix(), end.Unix(),
		)
	}
	sm2, err := s.DynamoDB.Fetch(name, start, end)
	if err != nil {
		return nil, errors.Wrapf(err,
			"Failed to FetchMetricsFromDynamoDB %s %d %d",
			name, start.Unix(), end.Unix(),
		)
	}
	sm := sm1.MergePointsToSlice(sm2)
	// TODO S3
	return sm, nil
}
