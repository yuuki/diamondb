package redis

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeFetch func(name string, start, end time.Time) (series.SeriesMap, error)
	FakePut   func(slot string, name string, p *metric.Datapoint) error
}

func (s *FakeReadWriter) Fetch(name string, start, end time.Time) (series.SeriesMap, error) {
	return s.FakeFetch(name, start, end)
}

func (r *FakeReadWriter) Put(slot string, name string, p *metric.Datapoint) error {
	return r.FakePut(slot, name, p)
}
