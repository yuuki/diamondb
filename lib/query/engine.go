package query

import (
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/model"
	"github.com/yuuki/diamondb/lib/storage"
)

func EvalTarget(target string, startTime, endTime time.Time) ([]*model.Metric, error) {
	expr, err := ParseTarget(target)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to ParseTarget %s", target)
	}
	return invokeExpr(expr, startTime, endTime)
}

func invokeExpr(expr Expr, startTime, endTime time.Time) ([]*model.Metric, error) {
	switch e := expr.(type) {
	case SeriesListExpr:
		return storage.FetchMetric(e.Literal, startTime, endTime)
	case FuncExpr:
		var (
			metricList []*model.Metric
			err        error
		)
		for _, expr := range e.SubExprs {
			switch expr.(type) {
			case BoolExpr:
				continue
			case NumberExpr:
				continue
			case StringExpr:
				continue
			}

			metricList, err = invokeExpr(expr, startTime, endTime)
			if err != nil {
				return nil, err
			}
		}
		if metricList != nil {
			switch e.Name {
			case "alias":
				metricList, err = doAlias(metricList, e.SubExprs[1:])
				if err != nil {
					return nil, errors.Wrap(err, "Failed to run `doAlias`")
				}
				return metricList, err
			case "averageSeries", "avg":
				metricList = doAverageSeries(metricList)
				return metricList, nil
			case "sumSeries", "sum":
				metricList = doSumSeries(metricList)
				return metricList, nil
			case "maxSeries":
				metricList = doMaxSeries(metricList)
				return metricList, nil
			case "multiplySeries":
				metricList = doMultiplySeries(metricList)
				return metricList, nil
			default:
				return nil, errors.Errorf("Unknown function %s", e.Name)
			}
		}
		return metricList, err
	default:
		return nil, errors.Errorf("Unknown expression %+v", expr)
	}
}
