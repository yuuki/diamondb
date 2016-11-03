package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsetTarget_SeriesListExpr(t *testing.T) {
	expr, err := ParseTarget("server.web1.load")
	if assert.NoError(t, err) {
		v, ok := expr.(SeriesListExpr)
		assert.True(t, ok, "expr should be SeriesListExpr")
		assert.Equal(t, "server.web1.load", v.Literal)
	}
}

func TestParsetTarget_FuncExpr(t *testing.T) {
	expr, err := ParseTarget("averageSeries(company.server.*.threads.busy)")
	if assert.NoError(t, err) {
		v1, ok1 := expr.(FuncExpr)
		if assert.True(t, ok1, "expr should be FuncExpr") {
			assert.Equal(t, "averageSeries", v1.Name)
		}
		v2, ok2 := v1.SubExprs[0].(SeriesListExpr)
		if assert.True(t, ok2, "expr should be SeriesListExpr") {
			assert.Equal(t, "company.server.*.threads.busy", v2.Literal)
		}
	}
}

func TestParsetTarget_FuncExprWithArg(t *testing.T) {
	expr, err := ParseTarget("alias(Sales.widgets.largeBlue,\"Large Blue Widgets\")")
	if assert.NoError(t, err) {
		v1, ok1 := expr.(FuncExpr)
		if assert.True(t, ok1, "expr should be FuncExpr") {
			assert.Equal(t, "alias", v1.Name)
		}
		assert.Equal(t, 2, len(v1.SubExprs))
		v2, ok2 := v1.SubExprs[0].(SeriesListExpr)
		if assert.True(t, ok2, "expr should be SeriesListExpr") {
			assert.Equal(t, "Sales.widgets.largeBlue", v2.Literal)
		}
		v3, ok3 := v1.SubExprs[1].(StringExpr)
		if assert.True(t, ok3, "expr should be StringExpr") {
			assert.Equal(t, "Large Blue Widgets", v3.Literal)
		}
	}
}

func TestParsetTarget_FuncExprWithBoolExpr(t *testing.T) {
	expr, err := ParseTarget("summarize(metric,\"13week\",\"avg\",true)")
	if assert.NoError(t, err) {
		v1, ok1 := expr.(FuncExpr)
		if assert.True(t, ok1, "expr should be FuncExpr") {
			assert.Equal(t, "summarize", v1.Name)
		}
		assert.Equal(t, 4, len(v1.SubExprs))
		v2, ok2 := v1.SubExprs[0].(SeriesListExpr)
		if assert.True(t, ok2, "expr should be SeriesListExpr") {
			assert.Equal(t, "metric", v2.Literal)
		}
		v3, ok3 := v1.SubExprs[1].(StringExpr)
		if assert.True(t, ok3, "expr should be StringExpr") {
			assert.Equal(t, "13week", v3.Literal)
		}
		v4, ok4 := v1.SubExprs[2].(StringExpr)
		if assert.True(t, ok4, "expr should be StringExpr") {
			assert.Equal(t, "avg", v4.Literal)
		}
		v5, ok5 := v1.SubExprs[3].(BoolExpr)
		if assert.True(t, ok5, "expr should be BoolExpr") {
			assert.True(t, v5.Literal)
		}
	}
}

func TestParsetTarget_FuncExprWithFuncExpr(t *testing.T) {
	expr, err := ParseTarget("summarize(nonNegativeDerivative(gauge.num_users),\"1week\")")
	if assert.NoError(t, err) {
		v1, ok1 := expr.(FuncExpr)
		if assert.True(t, ok1, "expr should be FuncExpr") {
			assert.Equal(t, "summarize", v1.Name)
		}
		assert.Equal(t, 2, len(v1.SubExprs))
		v2, ok2 := v1.SubExprs[0].(FuncExpr)
		if assert.True(t, ok2, "expr should be FuncExpr") {
			assert.Equal(t, 1, len(v2.SubExprs))
			assert.Equal(t, "nonNegativeDerivative", v2.Name)
			v3, ok3 := v2.SubExprs[0].(SeriesListExpr)
			if assert.True(t, ok3, "expr should be SeriesListExpr") {
				assert.Equal(t, "gauge.num_users", v3.Literal)
			}
		}
		v4, ok4 := v1.SubExprs[1].(StringExpr)
		if assert.True(t, ok4, "expr should be StringExpr") {
			assert.Equal(t, "1week", v4.Literal)
		}
	}
}
