package query

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/yuuki/diamondb/lib/series"
	"github.com/yuuki/diamondb/lib/storage"
)

// UnsupportedFunctionError represents the error of unsupported query function.
type UnsupportedFunctionError struct {
	funcName string
}

// Error returns the error message for UnsupportedFunctionError.
// UnsupportedFunctionError satisfies error interface.
func (e *UnsupportedFunctionError) Error() string {
	return fmt.Sprintf("Unsupported function %s", e.funcName)
}

type funcArg struct {
	expr        Expr
	seriesSlice series.SeriesSlice
}

type funcArgs []*funcArg

// EvalTarget evaluates the target. It parses the target into AST structure and fetches datapoints from storage.
//
// ex. target: "alias(sumSeries(server1.loadavg5,server2.loadavg5),\"server_loadavg5\")"
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
	case GroupSeriesExpr:
		joinedValues := make([]string, 0, len(e.ValueList))
		for _, value := range e.ValueList {
			joinedValues = append(joinedValues, e.Prefix+value+e.Postfix)
		}
		expr = SeriesListExpr{Literal: strings.Join(joinedValues, ",")}
		return invokeExpr(fetcher, expr, startTime, endTime)
	case FuncExpr:
		args := funcArgs{}
		for _, expr := range e.SubExprs {
			switch e2 := expr.(type) {
			case BoolExpr:
				args = append(args, &funcArg{expr: expr})
			case NumberExpr:
				args = append(args, &funcArg{expr: expr})
			case StringExpr:
				args = append(args, &funcArg{expr: expr})
			case SeriesListExpr, GroupSeriesExpr:
				ss, err := invokeExpr(fetcher, expr, startTime, endTime)
				if err != nil {
					return nil, errors.Wrapf(err, "Failed to invokeExpr %v %d %d", expr, startTime, endTime)
				}
				ex := SeriesListExpr{Literal: ss.FormattedName()}
				args = append(args, &funcArg{expr: ex, seriesSlice: ss})
			case FuncExpr:
				ss, err := invokeExpr(fetcher, expr, startTime, endTime)
				if err != nil {
					return nil, errors.Wrapf(err, "Failed to invokeExpr %v %d %d", expr, startTime, endTime)
				}
				// Regard FuncExpr as SeriesListExpr after process function
				ex := SeriesListExpr{Literal: fmt.Sprintf("%s(%s)", e2.Name, ss.FormattedName())}
				args = append(args, &funcArg{expr: ex, seriesSlice: ss})
			default:
				return nil, errors.Errorf("Unknown expression %+v", expr)
			}
		}
		switch e.Name {
		case "alias":
			ss, err := doAlias(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "group":
			ss, err := doGroup(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "averageSeries", "avg":
			ss, err := doAverageSeries(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "sumSeries", "sum":
			ss, err := doSumSeries(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "minSeries":
			ss, err := doMinSeries(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "maxSeries":
			ss, err := doMaxSeries(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "multiplySeries":
			ss, err := doMultiplySeries(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "divideSeries":
			ss, err := doDivideSeries(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "summarize":
			ss, err := doSummarize(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		default:
			return nil, &UnsupportedFunctionError{funcName: e.Name}
		}
	default:
		return nil, errors.Errorf("Unknown expression %+v", expr)
	}
}
