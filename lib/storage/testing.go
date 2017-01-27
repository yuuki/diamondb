package storage

import (
	"time"

	"github.com/yuuki/diamondb/lib/series"
)

// FakeFetcher is for stub testing
type FakeFetcher struct {
	Fetcher
	FakeFetch func(name string, start, end time.Time) (series.SeriesSlice, error)
}

func (s *FakeFetcher) Fetch(name string, start, end time.Time) (series.SeriesSlice, error) {
	return s.FakeFetch(name, start, end)
}
