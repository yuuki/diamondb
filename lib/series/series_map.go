package series

import "sort"

type SeriesMap map[string]*SeriesPoint

func (sm SeriesMap) SortedNames() []string {
	names := make([]string, 0, len(sm))
	for name, _ := range sm {
		names = append(names, name)
	}
	slices := sort.StringSlice(names)
	slices.Sort()
	return slices
}

func (sm1 SeriesMap) Merge(sm2 SeriesMap) SeriesMap {
	for name, s := range sm2 {
		sm1[name] = s
	}
	return sm1
}

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

func (sm1 SeriesMap) MergePointsToSlice(sm2 SeriesMap) SeriesSlice {
	sm := sm1.MergePointsToMap(sm2)
	ss := make(SeriesSlice, 0, len(sm))
	for _, name := range sm.SortedNames() {
		ss = append(ss, sm[name].ToSeries())
	}
	return ss
}
