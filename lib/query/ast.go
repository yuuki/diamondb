package query

// Token represents the token of the query expression.
type Token struct {
	Token   int
	Literal string
}

// Expr represents each of query expression.
type Expr interface{}

// BoolExpr provides Number expression.
type BoolExpr struct {
	Literal bool
}

// NumberExpr provides Number expression.
type NumberExpr struct {
	Literal float64
}

// StringExpr provides String expression.
type StringExpr struct {
	Literal string
}

// SeriesListExpr provides SeriesList expression.
type SeriesListExpr struct {
	Literal string
}

// GroupSeriesExpr provides grouping series expression.
type GroupSeriesExpr struct {
	Prefix    string
	ValueList []string
	Postfix   string
}

// FuncExpr provides function expression.
type FuncExpr struct {
	Name     string
	SubExprs []Expr
}
