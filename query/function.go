package query

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/yuuki/dynamond/mathutil"
	"github.com/yuuki/dynamond/model"
)

func zipSeriesList(seriesList []*model.Metric) (map[string][]float64, int) {
	if len(seriesList) < 1 {
		return nil, 0
	}

	maxLen := 0
	for _, series := range seriesList {
		size := len(series.DataPoints)
		if maxLen < size {
			maxLen = size
		}
	}

	valuesByTimeStamp := make(map[string][]float64)
	for i := 0; i < maxLen; i++ {
		values := make([]float64, 0, len(seriesList))
		for _, series := range seriesList {
			if i >= len(series.DataPoints) {
				continue
			}
			if series.DataPoints[i] == nil {
				continue
			}
			values = append(values, series.DataPoints[i].Value)
			ts := series.DataPoints[i].Timestamp
			// use type string as map index because cannot use type int64 as type int32 in map index
			valuesByTimeStamp[fmt.Sprintf("%d", ts)] = values
		}
	}
	return valuesByTimeStamp, maxLen
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

func normalize(seriesList []*model.Metric) ([]*model.Metric, int64, int64, int) {
	if len(seriesList) < 1 {
		return seriesList, 0, 0, 0
	}
	var (
		step	= seriesList[0].Step
		start	= seriesList[0].Start
		end	= seriesList[0].End
	)
	for _, series := range seriesList {
		step = mathutil.Lcm(step, series.Step)
		start = mathutil.MinInt64(start, series.Start)
		end = mathutil.MaxInt64(end, series.Start)
	}
	end -= (end - start) % int64(step)
	return seriesList, start, end, step
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
	return seriesList
}

func doAverageSeries(seriesList []*model.Metric) []*model.Metric {
	series := averageSeries(seriesList)
	seriesList = make([]*model.Metric, 1)
	seriesList[0] = series
	return seriesList
}

// http://graphite.readthedocs.io/en/latest/functions.html#graphite.render.functions.averageSeries
func averageSeries(seriesList []*model.Metric) *model.Metric {
	if len(seriesList) == 0 {
		return model.NewEmptyMetric()
	}
	name := fmt.Sprintf("averageSeries(%s)", formatSeries(seriesList))

	seriesList, _, _, step := normalize(seriesList)

	valuesByTimeStamp, maxLen := zipSeriesList(seriesList)
	points := make([]*model.DataPoint, 0, maxLen)
	for key, vals := range valuesByTimeStamp {
		avg := mathutil.SumFloat64(vals) / float64(len(vals))
		ts, _ := strconv.ParseInt(key, 10, 64)
		point := model.NewDataPoint(ts, avg)
		points = append(points, point)
	}
	return model.NewMetric(name, points, step)
}

