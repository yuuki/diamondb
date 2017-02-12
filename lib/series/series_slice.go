package series

import (
	"sort"
	"strings"

	"github.com/yuuki/diamondb/lib/mathutil"
)

// SeriesSlice represents a slice of Series.
type SeriesSlice []Series

// FormattedName returns the joined names in ss.
func (ss SeriesSlice) FormattedName() string {
	// Unique & Sort
	set := make(map[string]struct{})
	for _, s := range ss {
		set[s.Name()] = struct{}{}
	}
	names := make([]string, 0, len(ss))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// Normalize returns the minimum start timestamp, the largest end timestamp and the lcm step.
func (ss SeriesSlice) Normalize() (int64, int64, int) {
	if len(ss) < 1 {
		return 0, 0, 0
	}
	var (
		step  = ss[0].Step()
		start = ss[0].Start()
		end   = ss[0].End()
	)
	for _, s := range ss {
		step = mathutil.Lcm(step, s.Step())
		start = mathutil.MinInt64(start, s.Start())
		end = mathutil.MaxInt64(end, s.End())
	}
	end -= (end - start) % int64(step)
	return start, end, step
}

// Zip returns an iterator of a slice of values, where the i-th value contains the i-th element
// from each of ss. It is similar to izip in Python.
func (ss SeriesSlice) Zip() func() []float64 {
	zip := make([]float64, len(ss))
	i := 0
	return func() []float64 {
		if len(ss) == 0 {
			return nil
		}
		for j, series := range ss {
			if i >= series.Len() {
				return nil
			}
			zip[j] = series.Values()[i]
		}
		i++
		return zip
	}
}
