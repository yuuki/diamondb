package query

import (
	"fmt"
	"math"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/mathutil"
	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/timeparser"
)

func doAlias(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 2 {
		return nil, errors.New("too few arguments to function `alias`")
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, errors.New("invalid argument type `seriesList` to function `alias`.")
	}
	newNameExpr, ok := args[1].expr.(StringExpr)
	if !ok {
		return nil, errors.New("invalid argument type `newName` to function `alias`. `newName` must be string.")
	}
	return alias(args[0].seriesSlice, newNameExpr.Literal), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.alias
func alias(ss series.SeriesSlice, newName string) series.SeriesSlice {
	for _, series := range ss {
		series.SetAlias(newName)
	}
	return ss
}

func doSumSeries(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `sumSeries`")
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, errors.New("invalid argument type `seriesList` to function `sumSeries`.")
	}
	return series.SeriesSlice{sumSeries(args[0].seriesSlice)}, nil
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

func doAverageSeries(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `averageSeries`")
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, errors.New("invalid argument type `seriesList` to function `averageSeries`.")
	}
	return series.SeriesSlice{averageSeries(args[0].seriesSlice)}, nil
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

func doMinSeries(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `minSeries`")
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, errors.New("invalid argument type `seriesList` to function `minSeries`.")
	}
	return series.SeriesSlice{minSeries(args[0].seriesSlice)}, nil
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

func doMaxSeries(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `maxSeries`")
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, errors.New("invalid argument type `seriesList` to function `maxSeries`.")
	}
	return series.SeriesSlice{maxSeries(args[0].seriesSlice)}, nil
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

func doMultiplySeries(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 1 {
		return nil, errors.New("too few arguments to function `multiplySeries`")
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, errors.New("invalid argument type `seriesList` to function `multiplySeries`.")
	}
	return series.SeriesSlice{multiplySeries(args[0].seriesSlice)}, nil
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

func doDivideSeries(args funcArgs) (series.SeriesSlice, error) {
	if len(args) != 2 {
		return nil, errors.New("too few arguments to function `divideSeries`")
	}
	for i := 0; i < 2; i++ {
		_, ok := args[i].expr.(SeriesListExpr)
		if !ok {
			return nil, errors.New("invalid argument type `seriesList` to function `divideSeries`.")
		}
	}
	return divideSeries(args[0].seriesSlice, args[1].seriesSlice[0]), nil
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

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.summarize
func summarize(ss series.SeriesSlice, interval string, function string) (series.SeriesSlice, error) {
	result := make(series.SeriesSlice, 0, len(ss))
	delta, err := timeparser.ParseTimeOffset(interval)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	step := int64(delta.Seconds())
	for _, s := range ss {
		bucketNum := int(math.Ceil(float64((s.End() - s.Start()) / step)))
		buckets := make(map[string][]float64, bucketNum)
		for _, p := range s.Points() {
			t, val := p.Timestamp(), p.Value()
			bucketTime := t - (t % step)
			key := fmt.Sprintf("%d", bucketTime)
			if _, ok := buckets[key]; !ok {
				buckets[key] = []float64{}
			}
			if !math.IsNaN(val) {
				buckets[key] = append(buckets[key], val)
			}
		}
		newStart := s.Start() - (s.Start() % step)
		newEnd := s.End() - (s.End() % step) + step
		newValues := make([]float64, 0, bucketNum)
		for t := newStart; t <= newEnd; t += step {
			bucketTime := t - (t % step)
			key := fmt.Sprintf("%d", bucketTime)
			if bucketVals, ok := buckets[key]; !ok {
				newValues = append(newValues, math.NaN())
			} else {
				switch function {
				case "avg":
					avg := mathutil.SumFloat64(bucketVals) / float64(len(bucketVals))
					newValues = append(newValues, avg)
				case "last":
					newValues = append(newValues, bucketVals[len(bucketVals)-1])
				case "max":
					newValues = append(newValues, mathutil.MaxFloat64(bucketVals))
				case "min":
					newValues = append(newValues, mathutil.MinFloat64(bucketVals))
				case "sum":
					newValues = append(newValues, mathutil.SumFloat64(bucketVals))
				default:
					return nil, errors.Errorf("unsupported summarize function %s", function)
				}
			}
		}
		newName := fmt.Sprintf("summarize(%s, \"%s\", \"%s\")", s.Name(), interval, function)
		newSeries := series.NewSeries(newName, newValues, newStart, int(step))
		result = append(result, newSeries)
	}
	return result, nil
}
