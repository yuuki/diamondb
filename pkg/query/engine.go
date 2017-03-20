package query

import (
	"fmt"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"
	"github.com/yuuki/diamondb/pkg/model"
	"github.com/yuuki/diamondb/pkg/storage"
)

// UnsupportedFunctionError represents the error of unsupported query function.
type UnsupportedFunctionError struct {
	funcName string
}

// Error returns the error message for UnsupportedFunctionError.
// UnsupportedFunctionError satisfies error interface.
func (e *UnsupportedFunctionError) Error() string {
	return fmt.Sprintf("unsupported function %s", e.funcName)
}

type funcArg struct {
	expr        Expr
	seriesSlice model.SeriesSlice
}

// EvalTargets evaluates the targets concurrently. It is guaranteed that the order
// of the targets as input value and SeriesSlice as retuen value is the same.
func EvalTargets(reader storage.ReadWriter, targets []string, startTime, endTime time.Time) (model.SeriesSlice, error) {
	var eg errgroup.Group
	ordered := make([]model.SeriesSlice, len(targets))
	for i, target := range targets {
		i, target := i, target
		eg.Go(func() error {
			ss, err := EvalTarget(reader, target, startTime, endTime)
			if err != nil {
				return err
			}
			ordered[i] = ss
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	var results model.SeriesSlice
	for _, ss := range ordered {
		results = append(results, ss...)
	}
	return results, nil
}

// EvalTarget evaluates the target. It parses the target into AST structure and fetches datapoints from storage.
//
// ex. target: "alias(sumSeries(server1.loadavg5,server2.loadavg5),\"server_loadavg5\")"
func EvalTarget(reader storage.ReadWriter, target string, startTime, endTime time.Time) (model.SeriesSlice, error) {
	expr, err := ParseTarget(target)
	if err != nil {
		return nil, err
	}
	ss, err := invokeExpr(reader, expr, startTime, endTime)
	if err != nil {
		return nil, err
	}
	return ss, err
}

func invokeExpr(reader storage.ReadWriter, expr Expr, startTime, endTime time.Time) (model.SeriesSlice, error) {
	switch e := expr.(type) {
	case SeriesListExpr:
		ss, err := reader.Fetch(e.Literal, startTime, endTime)
		if err != nil {
			return nil, err
		}
		return ss, nil
	case GroupSeriesExpr:
		joinedValues := make([]string, 0, len(e.ValueList))
		for _, value := range e.ValueList {
			joinedValues = append(joinedValues, e.Prefix+value+e.Postfix)
		}
		expr = SeriesListExpr{Literal: strings.Join(joinedValues, ",")}
		ss, err := invokeExpr(reader, expr, startTime, endTime)
		if err != nil {
			return nil, err
		}
		return ss, nil
	case FuncExpr:
		var (
			ss  model.SeriesSlice
			err error
		)

		args, err := invokeSubExprs(reader, e.SubExprs, startTime, endTime)
		if err != nil {
			return nil, err
		}
		switch e.Name {
		case "alias":
			ss, err = doAlias(args)
		case "offset":
			ss, err = doOffset(args)
		case "scale":
			ss, err = doScale(args)
		case "group":
			ss, err = doGroup(args)
		case "averageSeries", "avg":
			ss, err = doAverageSeries(args)
		case "sumSeries", "sum":
			ss, err = doSumSeries(args)
		case "minSeries":
			ss, err = doMinSeries(args)
		case "maxSeries":
			ss, err = doMaxSeries(args)
		case "multiplySeries":
			ss, err = doMultiplySeries(args)
		case "divideSeries":
			ss, err = doDivideSeries(args)
		case "percentileOfSeries":
			ss, err = doPercentileOfSeries(args)
		case "summarize":
			ss, err = doSummarize(args)
		case "sumSeriesWithWildcards":
			ss, err = doSumSeriesWithWildcards(args)
		default:
			return nil, &UnsupportedFunctionError{funcName: e.Name}
		}
		if err != nil {
			return nil, err
		}
		return ss, err
	default:
		return nil, errors.Errorf("unknown expression (%s)", expr)
	}
}

func invokeSubExprs(reader storage.ReadWriter, exprs []Expr, startTime, endTime time.Time) ([]*funcArg, error) {
	type result struct {
		value *funcArg
		err   error
		index int
	}

	c := make(chan *result, len(exprs))
	args := make([]*funcArg, len(exprs))
	numTasks := 0

	for i, expr := range exprs {
		switch expr.(type) {
		case BoolExpr, NumberExpr, StringExpr:
			args[i] = &funcArg{expr: expr}
		case SeriesListExpr, GroupSeriesExpr, FuncExpr:
			numTasks++
			go func(e Expr, start, end time.Time, i int) {
				ss, err := invokeExpr(reader, e, start, end)
				c <- &result{
					value: &funcArg{
						expr:        SeriesListExpr{Literal: ss.FormattedName()},
						seriesSlice: ss,
					},
					err:   err,
					index: i,
				}
			}(expr, startTime, endTime, i)
		default:
			return nil, errors.Errorf("unknown expression (%s)", expr)
		}
	}

	for i := 0; i < numTasks; i++ {
		ret := <-c
		if ret.err != nil {
			// return err that is found firstly.
			return nil, ret.err
		}
		args[ret.index] = ret.value
	}

	return args, nil
}
