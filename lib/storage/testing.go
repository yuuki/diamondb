package storage

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeRead  func(name string, start, end time.Time) (series.SeriesSlice, error)
	FakeWrite func(*metric.Metric) error
}

// Read is for stub testing
func (s *FakeReadWriter) Read(name string, start, end time.Time) (series.SeriesSlice, error) {
	return s.FakeRead(name, start, end)
}

// Write is for stub testing
func (r *FakeReadWriter) Write(m *metric.Metric) error {
	return r.FakeWrite(m)
}
