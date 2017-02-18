package redis

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeRead  func(name string, start, end time.Time) (series.SeriesMap, error)
	FakeWrite func(slot string, name string, p *metric.Datapoint) error
}

// Read is for stub testing.
func (s *FakeReadWriter) Read(name string, start, end time.Time) (series.SeriesMap, error) {
	return s.FakeRead(name, start, end)
}

// Write is for stub testing.
func (r *FakeReadWriter) Write(slot string, name string, p *metric.Datapoint) error {
	return r.FakeWrite(slot, name, p)
}
