package storage

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage/redis"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeFetch        func(name string, start, end time.Time) (series.SeriesSlice, error)
	FakeInsertMetric func(*metric.Metric) error
}

func (s *FakeReadWriter) Fetch(name string, start, end time.Time) (series.SeriesSlice, error) {
	return s.FakeFetch(name, start, end)
}

func (r *FakeReadWriter) InsertMetric(m *metric.Metric) error {
	return r.FakeInsertMetric(m)
}

// FakeRedisWriter is for stub testing
type FakeRedisWriter struct {
	redis.ReadWriter
	FakeInsertDatapoint func(slot string, name string, p *metric.Datapoint) error
}

func (r *FakeRedisWriter) InsertDatapoint(slot string, name string, p *metric.Datapoint) error {
	return r.FakeInsertDatapoint(slot, name, p)
}
