package query

import (
	"github.com/pkg/errors"

	"github.com/yuuki/dynamond/model"
)

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
