package series

import (
	"sort"
	"strings"
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
