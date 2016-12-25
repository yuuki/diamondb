package query

import (
	"strings"
	"text/scanner"
	"unicode"

	"github.com/pkg/errors"
)

const (
	EOF = -1
)

type Lexer struct {
	scanner.Scanner
	e      *Error
	result Expr
}

type Error struct {
	Message string
	Column  int
}

var symTable = map[string]int{
	"(":             LBRACK,
	")":             RBRACK,

	"true":          TRUE,
	"false":         FALSE,

	"alias":         FUNC,
	"avg":           FUNC,
	"averageSeries": FUNC,
	"summarize":     FUNC,
	"nonNegativeDerivative": FUNC,
}

func (l *Lexer) Lex(lval *yySymType) int {
	token := int(l.Scan())
	tokstr := l.TokenText()

	if token == scanner.EOF {
		return EOF
	}
	if token == scanner.Int || token == scanner.Float {
		token = NUMBER
	}
	if token == scanner.Char || token == scanner.String {
		token = STRING
		tokstr = tokstr[1:len(tokstr)-1]
	}
	if v, ok := symTable[l.TokenText()]; ok {
		token = v
	}
	if token == scanner.Ident {
		token = SERIESLIST
	}
	lval.token = Token{Token: token, Literal: tokstr}
	return token
}

func (l *Lexer) Error(msg string) {
	l.e = &Error{Message: msg, Column: l.Column}
}

func isIdentRune(ch rune, i int) bool {
	return ch == '_' || ch == '.' || ch == ':' || ch == '-' || ch == '*' || ch == '[' || ch == ']' || ch == '{' || ch == '}' || ch == '%' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
}

func ParseTarget(target string) (Expr, error) {
	l := &Lexer{}
	l.Init(strings.NewReader(target))
	l.Mode &^= scanner.ScanRawStrings | scanner.ScanComments | scanner.SkipComments
	l.IsIdentRune = isIdentRune
	yyParse(l)
	if l.e != nil {
		return l.result, errors.Errorf("Failed to parse %s %s %d", target, l.e.Message, l.e.Column)
	} else {
		return l.result, nil
	}
}