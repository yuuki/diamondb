package storage

import (
	"time"

	"github.com/yuuki/diamondb/lib/series"
)

// FakeFetcher is for stub testing
type FakeFetcher struct {
	Fetcher
	FakeFetchSeriesSlice func(name string, start, end time.Time) (series.SeriesSlice, error)
}

func (s *FakeFetcher) FetchSeriesSlice(name string, start, end time.Time) (series.SeriesSlice, error) {
	return s.FakeFetchSeriesSlice(name, start, end)
}
