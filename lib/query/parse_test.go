package query

import "testing"

func TestParsetTarget_SeriesListExpr(t *testing.T) {
	expr, err := ParseTarget("server1.loadavg5")
	if err != nil {
		t.Fatalf("%s", err)
	}

	v, ok := expr.(SeriesListExpr)
	if !ok {
		t.Fatalf("expr %#v should be SeriesListExpr", v)
	}
	if v.Literal != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", v.Literal)
	}
}

func TestParsetTarget_FuncExpr(t *testing.T) {
	expr, err := ParseTarget("averageSeries(server1.loadavg5)")
	if err != nil {
		t.Fatalf("%s", err)
	}

	v1, ok1 := expr.(FuncExpr)
	if !ok1 {
		t.Fatalf("expr %#v should be FuncExpr", v1)
	}
	if v1.Name != "averageSeries" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "averageSeries", v1.Name)
	}

	v2, ok2 := v1.SubExprs[0].(SeriesListExpr)
	if !ok2 {
		t.Fatalf("expr %#v should be SeriesListExpr", v2)
	}
	if v2.Literal != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", v2.Literal)
	}
}

func TestParsetTarget_FuncExprWithArg(t *testing.T) {
	expr, err := ParseTarget("alias(server1.loadavg5,\"server01.loadavg5\")")
	if err != nil {
		t.Fatalf("%s", err)
	}

	v1, ok1 := expr.(FuncExpr)
	if !ok1 {
		t.Fatalf("expr %#v should be FuncExpr", v1)
	}
	if v1.Name != "alias" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "alias", v1.Name)
	}
	if l := len(v1.SubExprs); l != 2 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 2, l)
	}

	v2, ok2 := v1.SubExprs[0].(SeriesListExpr)
	if !ok2 {
		t.Fatalf("expr %#v should be SeriesListExpr", v2)
	}
	if v2.Literal != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", v2.Literal)
	}

	v3, ok3 := v1.SubExprs[1].(StringExpr)
	if !ok3 {
		t.Fatalf("expr %#v should be StringExpr", v3)
	}
	if v3.Literal != "server01.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server01.loadavg5", v3.Literal)
	}
}

func TestParsetTarget_FuncExprWithBoolExpr(t *testing.T) {
	expr, err := ParseTarget("summarize(server1.loadavg5,\"13week\",\"avg\",true)")
	if err != nil {
		t.Fatalf("%s", err)
	}

	v1, ok1 := expr.(FuncExpr)
	if !ok1 {
		t.Fatalf("expr %#v should be FuncExpr", v1)
	}
	if v1.Name != "summarize" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "summarize", v1.Name)
	}
	if l := len(v1.SubExprs); l != 4 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 4, l)
	}

	v2, ok2 := v1.SubExprs[0].(SeriesListExpr)
	if !ok2 {
		t.Fatalf("expr %#v should be SeriesListExpr", v2)
	}
	if v2.Literal != "server1.loadavg5" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "server1.loadavg5", v2.Literal)
	}

	v3, ok3 := v1.SubExprs[1].(StringExpr)
	if !ok3 {
		t.Fatalf("expr %#v should be StringExpr", v3)
	}
	if v3.Literal != "13week" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "13week", v3.Literal)
	}

	v4, ok4 := v1.SubExprs[2].(StringExpr)
	if !ok4 {
		t.Fatalf("expr %#v should be StringExpr", v4)
	}
	if v4.Literal != "avg" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "avg", v4.Literal)
	}

	v5, ok5 := v1.SubExprs[3].(BoolExpr)
	if !ok5 {
		t.Fatalf("expr %#v should be BoolExpr", v5)
	}
	if v5.Literal != true {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", true, v5.Literal)
	}
}

func TestParsetTarget_FuncExprWithFuncExpr(t *testing.T) {
	expr, err := ParseTarget("summarize(nonNegativeDerivative(gauge.num_users),\"1week\")")
	if err != nil {
		t.Fatalf("%s", err)
	}

	v1, ok1 := expr.(FuncExpr)
	if !ok1 {
		t.Fatalf("expr %#v should be FuncExpr", v1)
	}
	if v1.Name != "summarize" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "summarize", v1.Name)
	}
	if l := len(v1.SubExprs); l != 2 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 2, l)
	}

	v2, ok2 := v1.SubExprs[0].(FuncExpr)
	if !ok2 {
		t.Fatalf("expr %#v should be FuncExpr", v2)
	}
	if v2.Name != "nonNegativeDerivative" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "nonNegativeDerivative", v2.Name)
	}
	if l := len(v2.SubExprs); l != 1 {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", 1, l)
	}

	v3, ok3 := v2.SubExprs[0].(SeriesListExpr)
	if !ok3 {
		t.Fatalf("expr %#v should be SeriesListExpr", v3)
	}
	if v3.Literal != "gauge.num_users" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "gauge.num", v3.Literal)
	}
	v4, ok4 := v1.SubExprs[1].(StringExpr)
	if !ok4 {
		t.Fatalf("expr %#v should be StringExpr", v4)
	}
	if v4.Literal != "1week" {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", "1week", v4.Literal)
	}
}
