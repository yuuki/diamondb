package storage

import (
	"time"

	"github.com/yuuki/diamondb/pkg/metric"
	"github.com/yuuki/diamondb/pkg/model"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeFetch        func(name string, start, end time.Time) (model.SeriesSlice, error)
	FakeInsertMetric func(*metric.Metric) error
}

func (s *FakeReadWriter) Fetch(name string, start, end time.Time) (model.SeriesSlice, error) {
	return s.FakeFetch(name, start, end)
}

func (r *FakeReadWriter) InsertMetric(m *metric.Metric) error {
	return r.FakeInsertMetric(m)
}
