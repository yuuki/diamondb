package redis

import (
	"time"

	"github.com/yuuki/diamondb/lib/series"
)

// FakeFetcher is for stub testing
type FakeFetcher struct {
	ReadWriter
	FakeFetch func(name string, start, end time.Time) (series.SeriesMap, error)
}

func (s *FakeFetcher) Fetch(name string, start, end time.Time) (series.SeriesMap, error) {
	return s.FakeFetch(name, start, end)
}
