package redis

import (
	"time"

	"github.com/yuuki/diamondb/pkg/metric"
	"github.com/yuuki/diamondb/pkg/model"
)

// FakeReadWriter is for stub testing
type FakeReadWriter struct {
	ReadWriter
	FakeFetch func(name string, start, end time.Time) (model.SeriesMap, error)
	FakeGet   func(slot string, name string) (map[int64]float64, error)
	FakeLen   func(slot string, name string) (int64, error)
	FakePut   func(slot string, name string, p *metric.Datapoint) error
}

func (s *FakeReadWriter) Fetch(name string, start, end time.Time) (model.SeriesMap, error) {
	return s.FakeFetch(name, start, end)
}

func (r *FakeReadWriter) Get(slot string, name string) (map[int64]float64, error) {
	return r.FakeGet(slot, name)
}

func (r *FakeReadWriter) Len(slot string, name string) (int64, error) {
	return r.FakeLen(slot, name)
}

func (r *FakeReadWriter) Put(slot string, name string, p *metric.Datapoint) error {
	return r.FakePut(slot, name, p)
}
