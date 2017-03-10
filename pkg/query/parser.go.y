/* go tool yacc -o lib/query/parser.go lib/query/parser.go.y */
%{
package query

import (
        "strconv"
)
%}

%union{
  token token
  expr  Expr
  exprs []Expr
  target Expr
  str string
  literals []string
}

%type<target> target
%type<expr> expr
%type<exprs> exprs
%type<literals> identifiers
%type<str> identifier_opt ident_in_brace
%token<token> NUMBER STRING TRUE FALSE IDENTIFIER FUNC LBRACK RBRACK

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
    n, _ := strconv.ParseFloat($1.lit, 64)
    $$ = NumberExpr{Literal: n}
  }
  | STRING
  {
    $$ = StringExpr{Literal: $1.lit}
  }
  | IDENTIFIER
  {
    $$ = SeriesListExpr{Literal: $1.lit}
  }
  | IDENTIFIER '{' identifiers '}' identifier_opt
  {
    $$ = GroupSeriesExpr{Prefix: $1.lit, ValueList: $3, Postfix: $5}
  }
  | FUNC LBRACK exprs RBRACK
  {
    $$ = FuncExpr{Name: $1.lit, SubExprs: $3}
  }

exprs :
  expr
  {
    $$ = []Expr{$1}
  }
  | exprs ',' expr
  {
    $$ = append($1, $3)
  }

identifier_opt:
  {
    $$ = ""
  }
  | IDENTIFIER
  {
    $$ = $1.lit
  }

identifiers: 
  ident_in_brace 
  {
    $$ = []string{$1}
  }
  | identifiers ',' ident_in_brace
  {
    $$ = append($1, $3)
  }

ident_in_brace:
  NUMBER
  { 
    $$ = $1.lit
  }
  | IDENTIFIER 
  {
    $$ = $1.lit
  }

%%
