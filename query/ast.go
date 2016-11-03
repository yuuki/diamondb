package query

type Token struct {
	Token   int
	Literal string
}

type Expr interface{}

// BoolExpr provide Number expression.
type BoolExpr struct {
	Literal bool
}

// NumberExpr provide Number expression.
type NumberExpr struct {
	Literal int
}

// StringExpr provide String expression.
type StringExpr struct {
	Literal string
}

// SeriesListExpr provide SeriesList expression.
type SeriesListExpr struct {
	Literal string
}

// FunctionExpr provide function expression.
type FuncExpr struct {
	Name     string
	SubExprs []Expr
}
