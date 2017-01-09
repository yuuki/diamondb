package query

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage"
)

type UnsupportedFunctionError struct {
	funcName string
}

func (e *UnsupportedFunctionError) Error() string {
	return fmt.Sprintf("Unsupported function %s", e.funcName)
}

func EvalTarget(fetcher storage.Fetcher, target string, startTime, endTime time.Time) (series.SeriesSlice, error) {
	expr, err := ParseTarget(target)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to ParseTarget %s", target)
	}
	ss, err := invokeExpr(fetcher, expr, startTime, endTime)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to invokeExpr %v %d %d", expr, startTime, endTime)
	}
	return ss, err
}

func invokeExpr(fetcher storage.Fetcher, expr Expr, startTime, endTime time.Time) (series.SeriesSlice, error) {
	switch e := expr.(type) {
	case SeriesListExpr:
		ss, err := fetcher.FetchSeriesSlice(e.Literal, startTime, endTime)
		if err != nil {
			return nil, errors.Wrapf(err,
				"Failed to FetchSeriesSlice %s %d %d",
				e.Literal, startTime.Unix(), endTime.Unix(),
			)
		}
		return ss, nil
	case FuncExpr:
		var (
			ss  series.SeriesSlice
			err error
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

			ss, err = invokeExpr(fetcher, expr, startTime, endTime)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to invokeExpr %v %d %d", expr, startTime, endTime)
			}
		}
		if ss != nil {
			switch e.Name {
			case "alias":
				ss, err = doAlias(ss, e.SubExprs[1:])
				if err != nil {
					return nil, errors.Wrap(err, "Failed to doAlias")
				}
				return ss, err
			case "averageSeries", "avg":
				return series.SeriesSlice{averageSeries(ss)}, nil
			case "sumSeries", "sum":
				return series.SeriesSlice{sumSeries(ss)}, nil
			case "maxSeries":
				return series.SeriesSlice{maxSeries(ss)}, nil
			case "multiplySeries":
				return series.SeriesSlice{multiplySeries(ss)}, nil
			default:
				return nil, &UnsupportedFunctionError{funcName: e.Name}
			}
		}
		return ss, err
	default:
		return nil, errors.Errorf("Unknown expression %+v", expr)
	}
}
