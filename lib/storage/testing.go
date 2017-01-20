package storage

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

// FakeFetcher is for stub testing
type FakeFetcher struct {
	Fetcher
	FakeFetchSeriesSlice func(name string, start, end time.Time) (series.SeriesSlice, error)
}

func (s *FakeFetcher) FetchSeriesSlice(name string, start, end time.Time) (series.SeriesSlice, error) {
	return s.FakeFetchSeriesSlice(name, start, end)
}

// FakeWriter is for stub testing
type FakeWriter struct {
	FakeInsertMetric func(*metric.Metric) error
}

func (r *FakeWriter) InsertMetric(m *metric.Metric) error {
	return r.FakeInsertMetric(m)
}

// FakeRedisWriter is for stub testing
type FakeRedisWriter struct {
	redis.Writer
	FakeInsertDatapoint func(slot string, name string, p *metric.Datapoint) error
}

func (r *FakeRedisWriter) InsertDatapoint(slot string, name string, p *metric.Datapoint) error {
	return r.FakeInsertDatapoint(slot, name, p)
}
