package redis

import (
	"time"

	"github.com/yuuki/diamondb/lib/metric"
	"github.com/yuuki/diamondb/lib/series"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeFetch           func(name string, start, end time.Time) (series.SeriesMap, error)
	FakeInsertDatapoint func(slot string, name string, p *metric.Datapoint) error
}

func (s *FakeReadWriter) Fetch(name string, start, end time.Time) (series.SeriesMap, error) {
	return s.FakeFetch(name, start, end)
}

func (r *FakeReadWriter) InsertDatapoint(slot string, name string, p *metric.Datapoint) error {
	return r.FakeInsertDatapoint(slot, name, p)
}
