package query

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/yuuki/diamondb/pkg/mathutil"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage"
	"github.com/yuuki/diamondb/pkg/timeparser"
)

// ArgumentError represents an error of the argument of query functions.
type ArgumentError struct {
	funcName string
	msg      string
}

// Error returns the error message for ArgumentError.
func (e *ArgumentError) Error() string {
	return fmt.Sprintf("%s: %s", e.funcName, e.msg)
}

func doAlias(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) != 2 {
		return nil, &ArgumentError{
			funcName: "alias",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2)", len(args)),
		}
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "alias",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[0].expr),
		}
	}
	newNameExpr, ok := args[1].expr.(StringExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "alias",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[1].expr),
		}
	}
	return alias(args[0].seriesSlice, newNameExpr.Literal), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.alias
func alias(ss model.SeriesSlice, newName string) model.SeriesSlice {
	for _, series := range ss {
		series.SetAlias(newName)
	}
	return ss
}

func doOffset(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) != 2 {
		return nil, &ArgumentError{
			funcName: "offset",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2)", len(args)),
		}
	}

	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "offset",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[0].expr),
		}
	}
	factorExpr, ok := args[1].expr.(NumberExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "offset",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[1].expr),
		}
	}

	return offset(args[0].seriesSlice, float64(factorExpr.Literal)), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.offset
func offset(ss model.SeriesSlice, factor float64) model.SeriesSlice {
	result := make(model.SeriesSlice, 0, len(ss))
	for _, s := range ss {
		name := fmt.Sprintf("offset(%s,%g)", s.Name(), factor)
		vals := s.Values()
		for i := 0; i < len(vals); i++ {
			vals[i] += factor
		}
		result = append(result, model.NewSeries(name, vals, s.Start(), s.Step()))
	}
	return result
}

func doScale(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) != 2 {
		return nil, &ArgumentError{
			funcName: "scale",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2)", len(args)),
		}
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "scale",
			msg:      fmt.Sprintf("invalid argument type (%s) as SeriesList", args[0].expr),
		}
	}
	factor, ok := args[1].expr.(NumberExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "scale",
			msg:      fmt.Sprintf("invalid argument type (%s) as factor", args[1].expr),
		}
	}
	return scale(args[0].seriesSlice, factor.Literal), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.scale
func scale(ss model.SeriesSlice, factor float64) model.SeriesSlice {
	result := make(model.SeriesSlice, 0, len(ss))
	for _, s := range ss {
		name := fmt.Sprintf("scale(%s,%g)", s.Name(), factor)
		vals := s.Values()
		for i := 0; i < len(vals); i++ {
			vals[i] *= factor
		}
		result = append(result, model.NewSeries(name, vals, s.Start(), s.Step()))
	}
	return result
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.group
func doGroup(args []*funcArg) (model.SeriesSlice, error) {
	var ss model.SeriesSlice
	for i, arg := range args {
		_, ok := args[i].expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "group",
				msg:      fmt.Sprintf("invalid argument type (%s)", args[i].expr),
			}
		}
		ss = append(ss, arg.seriesSlice...)
	}
	return ss, nil
}

func doSumSeries(args []*funcArg) (model.SeriesSlice, error) {
	var ss model.SeriesSlice
	for i, arg := range args {
		_, ok := args[i].expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "sumSeries",
				msg:      fmt.Sprintf("invalid argument type (%s)", args[i].expr),
			}
		}
		ss = append(ss, arg.seriesSlice...)
	}
	return model.SeriesSlice{sumSeries(ss)}, nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.sumSeries
func sumSeries(ss model.SeriesSlice) *model.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		vals = append(vals, mathutil.SumFloat64(row))
	}
	name := fmt.Sprintf("sumSeries(%s)", ss.FormattedName())
	return model.NewSeries(name, vals, start, step)
}

func doAverageSeries(args []*funcArg) (model.SeriesSlice, error) {
	var ss model.SeriesSlice
	for _, arg := range args {
		_, ok := arg.expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "averageSeries",
				msg:      fmt.Sprintf("invalid argument type (%s)", arg.expr),
			}
		}
		ss = append(ss, arg.seriesSlice...)
	}
	return model.SeriesSlice{averageSeries(ss)}, nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.averageSeries
func averageSeries(ss model.SeriesSlice) *model.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		vals = append(vals, mathutil.AvgFloat64(row))
	}
	name := fmt.Sprintf("averageSeries(%s)", ss.FormattedName())
	return model.NewSeries(name, vals, start, step)
}

func doMinSeries(args []*funcArg) (model.SeriesSlice, error) {
	var ss model.SeriesSlice
	for _, arg := range args {
		_, ok := arg.expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "minSeries",
				msg:      fmt.Sprintf("invalid argument type (%s)", arg.expr),
			}
		}
		ss = append(ss, arg.seriesSlice...)
	}
	return model.SeriesSlice{minSeries(ss)}, nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.minSeries
func minSeries(ss model.SeriesSlice) *model.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		vals = append(vals, mathutil.MinFloat64(row))
	}
	name := fmt.Sprintf("minSeries(%s)", ss.FormattedName())
	return model.NewSeries(name, vals, start, step)
}

func doMaxSeries(args []*funcArg) (model.SeriesSlice, error) {
	var ss model.SeriesSlice
	for _, arg := range args {
		_, ok := arg.expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "maxSeries",
				msg:      fmt.Sprintf("invalid argument type (%s)", arg.expr),
			}
		}
		ss = append(ss, arg.seriesSlice...)
	}
	return model.SeriesSlice{maxSeries(ss)}, nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.maxSeries
func maxSeries(ss model.SeriesSlice) *model.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		avg := mathutil.MaxFloat64(row)
		vals = append(vals, avg)
	}
	name := fmt.Sprintf("maxSeries(%s)", ss.FormattedName())
	return model.NewSeries(name, vals, start, step)
}

func doMultiplySeries(args []*funcArg) (model.SeriesSlice, error) {
	var ss model.SeriesSlice
	for _, arg := range args {
		_, ok := arg.expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "multiplySeries",
				msg:      fmt.Sprintf("invalid argument type (%s)", arg.expr),
			}
		}
		ss = append(ss, arg.seriesSlice...)
	}
	return model.SeriesSlice{multiplySeries(ss)}, nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.multiplySeries
func multiplySeries(ss model.SeriesSlice) *model.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		avg := mathutil.MultiplyFloat64(row)
		vals = append(vals, avg)
	}
	name := fmt.Sprintf("multiplySeries(%s)", ss.FormattedName())
	return model.NewSeries(name, vals, start, step)
}

func doDivideSeries(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) != 2 {
		return nil, &ArgumentError{
			funcName: "divideSeries",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2)", len(args)),
		}
	}
	for i := 0; i < 2; i++ {
		_, ok := args[i].expr.(SeriesListExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "divideSeries",
				msg:      fmt.Sprintf("invalid argument type (%s)", args[i].expr),
			}
		}
	}
	return divideSeries(args[0].seriesSlice, args[1].seriesSlice[0]), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.divideSeries
func divideSeries(dividendSeriesSlice model.SeriesSlice, divisorSeries *model.Series) model.SeriesSlice {
	result := make(model.SeriesSlice, 0, len(dividendSeriesSlice))
	for _, s := range dividendSeriesSlice {
		bothSeriesSlice := model.SeriesSlice{s, divisorSeries}
		start, _, step := bothSeriesSlice.Normalize()
		vals := make([]float64, 0, len(bothSeriesSlice))
		iter := bothSeriesSlice.Zip()
		for row := iter(); row != nil; row = iter() {
			vals = append(vals, mathutil.DivideFloat64(row[0], row[1]))
		}
		name := fmt.Sprintf("divideSeries(%s,%s)", s.Name(), divisorSeries.Name())
		result = append(result, model.NewSeries(name, vals, start, step))
	}
	return result
}

func doPercentileOfSeries(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, &ArgumentError{
			funcName: "percentileOfSeries",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2,3)", len(args)),
		}
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "percentileOfSeries",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[0].expr),
		}
	}
	n, ok := args[1].expr.(NumberExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "percentileOfSeries",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[1].expr),
		}
	}
	interpolate := false
	if len(args) == 3 {
		i, ok := args[2].expr.(BoolExpr)
		if ok {
			interpolate = i.Literal
		}
	}
	ss := model.SeriesSlice{
		percentileOfSeries(args[0].seriesSlice, float64(n.Literal), interpolate),
	}
	return ss, nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.percentileOfSeries
func percentileOfSeries(ss model.SeriesSlice, n float64, interpolate bool) *model.Series {
	start, _, step := ss.Normalize()
	vals := make([]float64, 0, len(ss))
	iter := ss.Zip()
	for row := iter(); row != nil; row = iter() {
		vals = append(vals, mathutil.Percentile(row, n, interpolate))
	}
	name := fmt.Sprintf("percentileOfSeries(%s)", ss.FormattedName())
	return model.NewSeries(name, vals, start, step)
}

func doSummarize(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, &ArgumentError{
			funcName: "summarize",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2,3)", len(args)),
		}
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "summarize",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[0].expr),
		}
	}
	intervalExpr, ok := args[1].expr.(StringExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "summarize",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[1].expr),
		}
	}
	if len(args) == 3 {
		functionExpr, ok := args[2].expr.(StringExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "summarize",
				msg:      fmt.Sprintf("invalid argument type (%s)", args[2].expr),
			}
		}
		return summarize(args[0].seriesSlice, intervalExpr.Literal, functionExpr.Literal)
	}
	return summarize(args[0].seriesSlice, intervalExpr.Literal, "sum")
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.summarize
func summarize(ss model.SeriesSlice, interval string, function string) (model.SeriesSlice, error) {
	result := make(model.SeriesSlice, 0, len(ss))
	delta, err := timeparser.ParseTimeOffset(interval)
	if err != nil {
		return nil, err
	}
	step := int64(delta.Seconds())
	for _, s := range ss {
		bucketNum := int(float64((s.End() - s.Start()) / step))
		buckets := make(map[int64][]float64, bucketNum)
		for _, p := range s.Points() {
			t, val := p.Timestamp(), p.Value()
			bucketTime := t - (t % step)
			if _, ok := buckets[bucketTime]; !ok {
				buckets[bucketTime] = []float64{}
			}
			if !math.IsNaN(val) {
				buckets[bucketTime] = append(buckets[bucketTime], val)
			}
		}
		newStart := s.Start() - (s.Start() % step)
		newEnd := s.End() - (s.End() % step) + step
		newValues := make([]float64, 0, bucketNum)
		for t := newStart; t <= newEnd; t += step {
			bucketTime := t - (t % step)
			if bucketVals, ok := buckets[bucketTime]; !ok {
				newValues = append(newValues, math.NaN())
			} else {
				switch function {
				case "avg":
					avg := mathutil.AvgFloat64(bucketVals)
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
					return nil, &ArgumentError{
						funcName: "summarize",
						msg:      fmt.Sprintf("unsupported function error (%s)", function),
					}
				}
			}
		}
		newName := fmt.Sprintf("summarize(%s, \"%s\", \"%s\")", s.Name(), interval, function)
		newSeries := model.NewSeries(newName, newValues, newStart, int(step))
		result = append(result, newSeries)
	}
	return result, nil
}

func doSumSeriesWithWildcards(args []*funcArg) (model.SeriesSlice, error) {
	if len(args) < 2 {
		return nil, &ArgumentError{
			funcName: "sumSeriesWithWildcards",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 2+)", len(args)),
		}
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "sumSeriesWithWildcards",
			msg:      fmt.Sprintf("invalid argument type (%s)", args[0].expr),
		}
	}
	positions := make([]int, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		p, ok := args[i].expr.(NumberExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "sumSeriesWithWildcards",
				msg:      fmt.Sprintf("invalid argument type (%s)", args[i].expr),
			}
		}
		positions = append(positions, int(p.Literal))
	}
	return sumSeriesWithWildcards(args[0].seriesSlice, positions), nil
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.sumSeriesWithWildcards
func sumSeriesWithWildcards(ss model.SeriesSlice, positions []int) model.SeriesSlice {
	newSeries := make(map[string]*model.Series, len(ss))
	newNames := make([]string, 0, len(ss))
	for _, s := range ss {
		nameParts := []string{}
		for i, part := range strings.Split(s.Name(), ".") {
			inPosition := false
			for _, pos := range positions {
				if pos == i {
					inPosition = true
					break
				}
			}
			if inPosition {
				continue
			}
			nameParts = append(nameParts, part)
		}
		newName := strings.Join(nameParts, ".")
		if _, ok := newSeries[newName]; ok {
			newSeries[newName] = sumSeries(model.SeriesSlice{s, newSeries[newName]})
		} else {
			newSeries[newName] = s
			newNames = append(newNames, newName)
		}
		newSeries[newName].SetName(newName)
	}
	results := make(model.SeriesSlice, 0, len(newSeries))
	for _, name := range newNames {
		results = append(results, newSeries[name])
	}
	return results
}

func doLinerRegression(reader storage.ReadWriter, args []*funcArg, startTime, endTime time.Time) (model.SeriesSlice, error) {
	if len(args) == 0 || len(args) > 3 {
		return nil, &ArgumentError{
			funcName: "linearRegression",
			msg:      fmt.Sprintf("wrong number of arguments (%d for 1,2,3)", len(args)),
		}
	}
	_, ok := args[0].expr.(SeriesListExpr)
	if !ok {
		return nil, &ArgumentError{
			funcName: "linearRegression",
			msg:      fmt.Sprintf("invalid argument type (%s) as SeriesSlice", args[0].expr),
		}
	}
	var (
		startSourceAt, endSourceAt = startTime, endTime
		err                        error
	)
	if len(args) >= 2 {
		t, ok := args[1].expr.(StringExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "linearRegression",
				msg:      fmt.Sprintf("invalid argument type (%s) as startSourceAt", args[1].expr),
			}
		}
		startSourceAt, err = timeparser.ParseAtTime(t.Literal)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if len(args) >= 3 {
		t, ok := args[2].expr.(StringExpr)
		if !ok {
			return nil, &ArgumentError{
				funcName: "linearRegression",
				msg:      fmt.Sprintf("invalid argument type (%s) as endSourceAt", args[2].expr),
			}
		}
		endSourceAt, err = timeparser.ParseAtTime(t.Literal)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return linearRegression(reader, args[0].seriesSlice, startSourceAt, endSourceAt)
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.linearRegression
func linearRegression(fetcher storage.ReadWriter, ss model.SeriesSlice, startSourceAt, endSourceAt time.Time) (model.SeriesSlice, error) {
	targets := make([]string, 0, len(ss))
	for _, s := range ss {
		targets = append(targets, s.Name())
	}
	sourceSlice, err := EvalTargets(fetcher, targets, startSourceAt, endSourceAt)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to evaluate targets(%s)", strings.Join(targets, ","))
	}

	length := mathutil.MinInt64(int64(len(ss)), int64(len(sourceSlice)))
	results := make(model.SeriesSlice, 0, length)
	for i := 0; i < int(length); i++ {
		source, s := sourceSlice[i], ss[i]
		factor, offset := mathutil.LinearRegressionAnalysis(
			source.Values(), source.Start(), source.Step(),
		)
		if math.IsNaN(factor) || math.IsNaN(offset) {
			continue
		}
		vals := make([]float64, 0, s.Len())
		for j := 0; j < s.Len(); j++ {
			t := s.Start() + int64(s.Step()*j)
			vals = append(vals, offset+float64(t)*factor)
		}
		newName := fmt.Sprintf("linearRegression(%s, %d, %d)",
			s.Name(), startSourceAt.Unix(), endSourceAt.Unix(),
		)
		results = append(results, model.NewSeries(newName, vals, s.Start(), s.Step()))
	}
	return results, nil
}
