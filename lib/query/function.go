package query

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/mathutil"
	"github.com/yuuki/diamondb/lib/series"
)

func doAlias(ss series.SeriesSlice, args []Expr) (series.SeriesSlice, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `alias`")
	}
	newNameExpr, ok := args[0].(StringExpr)
	if !ok {
		return nil, errors.New("Invalid argument type `newName` to function `alias`. `newName` must be string.")
	}
	return alias(ss, newNameExpr.Literal), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.alias
func alias(ss series.SeriesSlice, newName string) series.SeriesSlice {
	for _, series := range ss {
		series.SetAlias(newName)
	}
	return ss
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.sumSeries
func sumSeries(ss series.SeriesSlice) series.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		vals = append(vals, mathutil.SumFloat64(row))
	}
	name := fmt.Sprintf("sumSeries(%s)", ss.FormatedName())
	return series.NewSeries(name, vals, start, step)
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.averageSeries
func averageSeries(ss series.SeriesSlice) series.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		avg := mathutil.SumFloat64(row) / float64(len(row))
		vals = append(vals, avg)
	}
	name := fmt.Sprintf("averageSeries(%s)", ss.FormatedName())
	return series.NewSeries(name, vals, start, step)
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.minSeries
func minSeries(ss series.SeriesSlice) series.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		vals = append(vals, mathutil.MinFloat64(row))
	}
	name := fmt.Sprintf("minSeries(%s)", ss.FormatedName())
	return series.NewSeries(name, vals, start, step)
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.maxSeries
func maxSeries(ss series.SeriesSlice) series.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		avg := mathutil.MaxFloat64(row)
		vals = append(vals, avg)
	}
	name := fmt.Sprintf("maxSeries(%s)", ss.FormatedName())
	return series.NewSeries(name, vals, start, step)
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.multiplySeries
func multiplySeries(ss series.SeriesSlice) series.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		avg := mathutil.MultiplyFloat64(row)
		vals = append(vals, avg)
	}
	name := fmt.Sprintf("multiplySeries(%s)", ss.FormatedName())
	return series.NewSeries(name, vals, start, step)
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.divideSeries
func divideSeries(dividendSeriesSlice series.SeriesSlice, divisorSeries series.Series) series.SeriesSlice {
	result := make(series.SeriesSlice, 0, len(dividendSeriesSlice))
	for _, s := range dividendSeriesSlice {
		bothSeriesSlice := series.SeriesSlice{s, divisorSeries}
		start, _, step := bothSeriesSlice.Normalize()
		vals := make([]float64, 0, len(bothSeriesSlice))
		iter := bothSeriesSlice.Zip()
		for row := iter(); row != nil; row = iter() {
			vals = append(vals, mathutil.DivideFloat64(row[0], row[1]))
		}
		name := fmt.Sprintf("divideSeries(%s,%s)", s.Name(), divisorSeries.Name())
		result = append(result, series.NewSeries(name, vals, start, step))
	}
	return result
}
