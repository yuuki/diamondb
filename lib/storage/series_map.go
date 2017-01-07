package storage

import (
	"sort"

	"github.com/yuuki/diamondb/lib/series"
)

type seriesMap map[string]*seriesPoint

func (sm seriesMap) SortedNames() []string {
	names := make([]string, 0, len(sm))
	for name, _ := range sm {
		names = append(names, name)
	}
	slices := sort.StringSlice(names)
	slices.Sort()
	return slices
}

func (sm1 seriesMap) Merge(sm2 seriesMap) seriesMap {
	for name, s := range sm2 {
		sm1[name] = s
	}
	return sm1
}

func (sm1 seriesMap) MergePointsToMap(sm2 seriesMap) seriesMap {
	for name, s1 := range sm1 {
		if s2, ok := sm2[name]; ok {
			points := append(s1.Points(), s2.Points()...)
			sm1[name] = newSeriesPoint(name, points, s1.Step())
		}
	}
	for name, s2 := range sm2 {
		if _, ok := sm1[name]; !ok {
			sm1[name] = s2
		}
	}
	return sm1
}

func (sm1 seriesMap) MergePointsToSlice(sm2 seriesMap) series.SeriesSlice {
	sm := sm1.MergePointsToMap(sm2)
	ss := make(series.SeriesSlice, 0, len(sm1))
	for _, name := range sm.SortedNames() {
		ss = append(ss, sm[name].ToSeries())
	}
	return ss
}
