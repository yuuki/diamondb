package query

import (
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/yuuki/dynamond/model"
)

func minInt32(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

func maxInt32(x, y int32) int32 {
	if x > y {
		return x
	}
	return y
}

// gcd is Greatest common divisor
func gcd(a, b int) int {
	if b == 0 {
		return a
	}
	return gcd(b, a % b)
}

// lcm is Least common multiple
func lcm(a, b int) int {
	if a == b {
		return a
	}
	if a < b {
		a, b = b, a // ensure a > b
	}
	return a * b / gcd(a, b)
}

func formatSeries(seriesList []*model.Metric) string {
	// Unique & Sort
	set := make(map[string]struct{})
	for _, s := range seriesList {
		set[s.Name] = struct{}{}
	}
	series := make([]string, 0, len(seriesList))
	for name := range set {
		series = append(series, name)
	}
	sort.Strings(series)
	return strings.Join(series, ",")
}

func doAlias(seriesList []*model.Metric, args []Expr) ([]*model.Metric, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `alias`")
	}
	newNameExpr, ok := args[0].(StringExpr)
	if !ok {
		return nil, errors.New("Invalid argument type `newName` to function `alias`. `newName` must be string.")
	}
	return alias(seriesList, newNameExpr.Literal), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.alias
func alias(seriesList []*model.Metric, newName string) []*model.Metric {
	for _, series := range seriesList {
		series.Name = newName
	}
	return seriesList, nil
}
