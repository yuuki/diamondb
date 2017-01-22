package series

import "sort"

// SeriesMap represents the map the series name to SeriesPoint to deduplicate the series
// with the same name.
type SeriesMap map[string]*SeriesPoint

// SortedNames returns the sorted slice of name.
func (sm SeriesMap) SortedNames() []string {
	names := make([]string, 0, len(sm))
	for name, _ := range sm {
		names = append(names, name)
	}
	slices := sort.StringSlice(names)
	slices.Sort()
	return slices
}

// Merge merges sm2 into sm1. Should not merge the two of SeriesMap including the series of
// the same name because Merge just overwrites.
func (sm1 SeriesMap) Merge(sm2 SeriesMap) SeriesMap {
	for name, s := range sm2 {
		sm1[name] = s
	}
	return sm1
}

// MergePointsToMap merges sm2 into sm1 in view of DataPoints.
func (sm1 SeriesMap) MergePointsToMap(sm2 SeriesMap) SeriesMap {
	for name, s1 := range sm1 {
		if s2, ok := sm2[name]; ok {
			points := append(s1.Points(), s2.Points()...)
			sm1[name] = NewSeriesPoint(name, points, s1.Step())
		}
	}
	for name, s2 := range sm2 {
		if _, ok := sm1[name]; !ok {
			sm1[name] = s2
		}
	}
	return sm1
}

// MergePointsToSlice returns SeriesSlice merged sm2 into sm1 in view of DataPoints. .
func (sm1 SeriesMap) MergePointsToSlice(sm2 SeriesMap) SeriesSlice {
	sm := sm1.MergePointsToMap(sm2)
	ss := make(SeriesSlice, 0, len(sm))
	for _, name := range sm.SortedNames() {
		ss = append(ss, sm[name].ToSeries())
	}
	return ss
}
