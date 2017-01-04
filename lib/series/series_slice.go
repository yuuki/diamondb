package series

import (
	"sort"
	"strings"

	"github.com/yuuki/diamondb/lib/mathutil"
)

type SeriesSlice []Series

func (ss SeriesSlice) FormatedName() string {
	// Unique & Sort
	set := make(map[string]bool)
	for _, s := range ss {
		set[s.Name()] = true
	}
	names := make([]string, 0, len(ss))
	for name, _ := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

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
