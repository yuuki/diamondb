package query

import (
	"fmt"
	"strings"
	"time"

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

type UnknownExpressionError struct {
	expr Expr
}

func (e *UnknownExpressionError) Error() string {
	return fmt.Sprintf("unknown expression %v", e.expr)
}

type funcArg struct {
	expr        Expr
	seriesSlice model.SeriesSlice
}

type funcArgs []*funcArg

// EvalTargets evaluates the targets concurrently. It is guaranteed that the order
// of the targets as input value and SeriesSlice as retuen value is the same.
func EvalTargets(reader storage.ReadWriter, targets []string, startTime, endTime time.Time) (model.SeriesSlice, error) {
	type result struct {
		value model.SeriesSlice
		err   error
		index int
	}

	c := make(chan *result)
	for i, target := range targets {
		go func(target string, start, end time.Time, i int) {
			ss, err := EvalTarget(reader, target, start, end)
			c <- &result{value: ss, err: err, index: i}
		}(target, startTime, endTime, i)
	}
	ordered := make([]model.SeriesSlice, len(targets))
	for i := 0; i < len(targets); i++ {
		ret := <-c
		if ret.err != nil {
			// return err that is found firstly.
			return nil, errors.Wrapf(ret.err, "failed to evaluate target (%s)", targets[i])
		}
		ordered[ret.index] = ret.value
	}
	results := model.SeriesSlice{}
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
		return nil, errors.Wrapf(err, "failed to parse target (%s)", target)
	}
	ss, err := invokeExpr(reader, expr, startTime, endTime)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to invoke %s", expr)
	}
	return ss, err
}

func invokeExpr(reader storage.ReadWriter, expr Expr, startTime, endTime time.Time) (model.SeriesSlice, error) {
	switch e := expr.(type) {
	case SeriesListExpr:
		ss, err := reader.Fetch(e.Literal, startTime, endTime)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to fetch (%s,%d,%d)", e.Literal, startTime.Unix(), endTime.Unix())
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
			return nil, errors.Wrapf(err, "failed to invoke (%s,%d,%d)", e, startTime.Unix(), endTime.Unix())
		}
		return ss, nil
	case FuncExpr:
		args, err := invokeSubExprs(reader, e.SubExprs, startTime, endTime)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to invoke arguments of (%s) function", e.Name)
		}
		switch e.Name {
		case "alias":
			ss, err := doAlias(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		case "offset":
			ss, err := doOffset(args)
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
		case "percentileOfSeries":
			ss, err := doPercentileOfSeries(args)
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
		case "sumSeriesWithWildcards":
			ss, err := doSumSeriesWithWildcards(args)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return ss, err
		default:
			return nil, &UnsupportedFunctionError{funcName: e.Name}
		}
	default:
		return nil, &UnknownExpressionError{expr: expr}
	}
}

func invokeSubExprs(reader storage.ReadWriter, exprs []Expr, startTime, endTime time.Time) (funcArgs, error) {
	type result struct {
		value *funcArg
		err   error
		index int
	}

	c := make(chan *result, len(exprs))
	args := make(funcArgs, len(exprs))
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
			return nil, &UnknownExpressionError{expr: expr}
		}
	}

	for i := 0; i < numTasks; i++ {
		ret := <-c
		if ret.err != nil {
			// return err that is found firstly.
			return nil, errors.Wrapf(ret.err,
				"failed to fetch concurrently (%d, %v)", i, ret.value.expr,
			)
		}
		args[ret.index] = ret.value
	}

	return args, nil
}
