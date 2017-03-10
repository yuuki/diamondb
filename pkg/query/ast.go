package query

import (
	"fmt"
	"strings"
)

// token represents the token of the query expression.
type token struct {
	tok int
	lit string
}

// Expr represents each of query expression.
type Expr interface{}

// BoolExpr provides Number expression.
type BoolExpr struct {
	Literal bool
}

func (e BoolExpr) String() string {
	return fmt.Sprintf("%t", e.Literal)
}

// NumberExpr provides Number expression.
type NumberExpr struct {
	Literal float64
}

func (e NumberExpr) String() string {
	return fmt.Sprintf("%g", e.Literal)
}

// StringExpr provides String expression.
type StringExpr struct {
	Literal string
}

func (e StringExpr) String() string {
	return e.Literal
}

// SeriesListExpr provides SeriesList expression.
type SeriesListExpr struct {
	Literal string
}

func (e SeriesListExpr) String() string {
	return e.Literal
}

// GroupSeriesExpr provides grouping series expression.
type GroupSeriesExpr struct {
	Prefix    string
	ValueList []string
	Postfix   string
}

func (e GroupSeriesExpr) String() string {
	vals := strings.Join(e.ValueList, ",")
	return fmt.Sprintf(e.Prefix + "{" + vals + "}" + e.Postfix)
}

// FuncExpr provides function expression.
type FuncExpr struct {
	Name     string
	SubExprs []Expr
}

func (e FuncExpr) String() string {
	return e.Name
}
