package util

import (
	"strings"
)

func GroupNames(names []string, count int) [][]string {
	nameGroups := make([][]string, 0, (len(names)+count-1)/count)
	for i, name := range names {
		if i%count == 0 {
			nameGroups = append(nameGroups, []string{})
		}
		nameGroups[len(nameGroups)-1] = append(nameGroups[len(nameGroups)-1], name)
	}
	return nameGroups
}

// roleA.r.{1,2,3,4}.loadavg
func SplitName(name string) []string {
	open := strings.IndexRune(name, '{')
	close := strings.IndexRune(name, '}')
	var names []string
	if open >= 0 && close >= 0 {
		prefix := name[0:open]
		indices := name[open+1 : close]
		suffix := name[close+1:]
		for _, i := range strings.Split(indices, ",") {
			names = append(names, prefix+i+suffix)
		}
	} else {
		names = strings.Split(name, ",")
	}
	return names
}
