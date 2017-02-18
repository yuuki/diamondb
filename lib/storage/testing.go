package storage

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
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
