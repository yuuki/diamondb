%{
package query

import (
        "strconv"
)
%}

%union{
  token Token
	expr  Expr
  exprs []Expr
  target Expr
}

%type<target> target
%type<expr> expr
%type<exprs> exprs
%token<token> NUMBER STRING TRUE FALSE SERIESLIST FUNC LBRACK RBRACK

%%

target :
  expr
  {
    $$ = $1
    yylex.(*Lexer).result = $$
  }

expr :
  TRUE
  {
    $$ = BoolExpr{Literal: true}
  }
  | FALSE
  {
    $$ = BoolExpr{Literal: false}
  }
  | NUMBER
  {
    n, _ := strconv.Atoi($1.Literal)
    $$ = NumberExpr{Literal: n}
  }
  | STRING
  {
    $$ = StringExpr{Literal: $1.Literal}
  }
  | SERIESLIST
  {
    $$ = SeriesListExpr{Literal: $1.Literal}
  }
  | FUNC LBRACK exprs RBRACK
  {
    $$ = FuncExpr{Name: $1.Literal, SubExprs: $3}
  }

exprs :
  expr
	{
		$$ = []Expr{$1}
	}
	| exprs ',' exprs
	{
		$$ = append($1, $3...)
	}

%%
