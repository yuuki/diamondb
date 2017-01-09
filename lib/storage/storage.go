package storage

import (
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/dynamo"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

type Fetcher interface {
	FetchSeriesSlice(string, time.Time, time.Time) (series.SeriesSlice, error)
}

type Store struct {
	Redis *redis.Redis
	// dynamodb client
	// s3 client
}

func NewStore() Fetcher {
	return &Store{
		Redis: redis.NewRedis(),
	}
}

func (s *Store) FetchSeriesSlice(name string, start, end time.Time) (series.SeriesSlice, error) {
	sm1, err := s.Redis.FetchSeriesMap(name, start, end)
	if err != nil {
		return nil, errors.Wrapf(err,
			"Failed to redis.FetchMetrics %s %d %d",
			name, start.Unix(), end.Unix(),
		)
	}
	sm2, err := dynamo.FetchSeriesMap(name, start, end)
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
