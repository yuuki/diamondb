package query

//go:generate goyacc -o parser.go parser.go.y

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/pkg/errors"
)

const (
	// EOF is End Of File.
	EOF = -1
)

// Lexer provides the argument of yyParse.
type Lexer struct {
	scanner.Scanner
	target string
	err    error
	result Expr
}

// ParserError represents the error of query parser.
type ParserError struct {
	Msg    string
	Column int
	Target string
}

// Error returns the error message for ParserError.
func (e *ParserError) Error() string {
	return fmt.Sprintf("failed to parse (%s,%s,%d)", e.Target, e.Msg, e.Column)
}

var (
	symTable = map[string]int{
		"(": LBRACK,
		")": RBRACK,

		"true":  TRUE,
		"false": FALSE,

		"alias":                  FUNC,
		"offset":                 FUNC,
		"group":                  FUNC,
		"avg":                    FUNC,
		"averageSeries":          FUNC,
		"sum":                    FUNC,
		"sumSeries":              FUNC,
		"minSeries":              FUNC,
		"maxSeries":              FUNC,
		"multiplySeries":         FUNC,
		"divideSeries":           FUNC,
		"percentileOfSeries":     FUNC,
		"summarize":              FUNC,
		"sumSeriesWithWildcards": FUNC,
		"nonNegativeDerivative":  FUNC,
	}
)

// Lex returns the token number for the yacc parser.
func (l *Lexer) Lex(lval *yySymType) int {
	token := int(l.Scan())
	tokstr := l.TokenText()

	if token == scanner.EOF {
		return EOF
	}
	if token == scanner.Char || token == scanner.String {
		token = STRING
		tokstr = tokstr[1 : len(tokstr)-1]
	}
	if v, ok := symTable[l.TokenText()]; ok {
		token = v
	}
	if token == scanner.Ident {
		token = IDENTIFIER
		if _, err := strconv.ParseFloat(tokstr, 64); err == nil {
			token = NUMBER
		}
	}
	lval.token = Token{Token: token, Literal: tokstr}
	return token
}

// Error returns the error message of parser.
func (l *Lexer) Error(msg string) {
	l.err = errors.WithStack(&ParserError{Target: l.target, Msg: msg, Column: l.Column})
}

func isIdentRune(ch rune, i int) bool {
	return ch == '_' || ch == '.' || ch == ':' || ch == '-' || ch == '*' || ch == '[' || ch == ']' || ch == '%' || unicode.IsLetter(ch) || unicode.IsDigit(ch)
}

// ParseTarget parses target string into the AST structure.
func ParseTarget(target string) (Expr, error) {
	l := &Lexer{}
	l.Init(strings.NewReader(target))
	l.Mode &^= scanner.ScanInts | scanner.ScanFloats | scanner.ScanRawStrings | scanner.ScanComments | scanner.SkipComments
	l.IsIdentRune = isIdentRune
	yyParse(l)
	if l.err != nil {
		return l.result, l.err
	}
	return l.result, nil
}
