package cypher

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// TokenType represents the type of a lexer token.
type TokenType int

const (
	// Literals
	TokenIdent   TokenType = iota // identifier
	TokenString                   // "string"
	TokenInt                      // 123
	TokenFloat                    // 1.23
	TokenTrue                     // true
	TokenFalse                    // false
	TokenNull                     // null

	// Keywords
	TokenMatch
	TokenOptional
	TokenWhere
	TokenReturn
	TokenCreate
	TokenDelete
	TokenDetach
	TokenSet
	TokenWith
	TokenAs
	TokenOrder
	TokenBy
	TokenLimit
	TokenSkipKw
	TokenAnd
	TokenOr
	TokenNot
	TokenIn
	TokenContains
	TokenStarts
	TokenEnds
	TokenIs
	TokenDistinct
	TokenCount
	TokenCollect
	TokenSum
	TokenAvg
	TokenMin
	TokenMax
	TokenSize
	TokenShortestPath
	TokenDesc
	TokenAsc
	TokenUnwind

	// Symbols
	TokenLParen    // (
	TokenRParen    // )
	TokenLBracket  // [
	TokenRBracket  // ]
	TokenLBrace    // {
	TokenRBrace    // }
	TokenColon     // :
	TokenDot       // .
	TokenComma     // ,
	TokenPipe      // |
	TokenStar      // *
	TokenDotDot    // ..
	TokenEq        // =
	TokenNeq       // <>
	TokenLt        // <
	TokenGt        // >
	TokenLte       // <=
	TokenGte       // >=
	TokenPlus      // +
	TokenMinus     // -
	TokenSlash     // /
	TokenPercent   // %
	TokenDash      // - (in patterns)
	TokenArrowR    // ->
	TokenArrowL    // <-
	TokenDashDash  // --

	TokenEOF
)

var keywords = map[string]TokenType{
	"MATCH":        TokenMatch,
	"OPTIONAL":     TokenOptional,
	"WHERE":        TokenWhere,
	"RETURN":       TokenReturn,
	"CREATE":       TokenCreate,
	"DELETE":       TokenDelete,
	"DETACH":       TokenDetach,
	"SET":          TokenSet,
	"WITH":         TokenWith,
	"AS":           TokenAs,
	"ORDER":        TokenOrder,
	"BY":           TokenBy,
	"LIMIT":        TokenLimit,
	"SKIP":         TokenSkipKw,
	"AND":          TokenAnd,
	"OR":           TokenOr,
	"NOT":          TokenNot,
	"IN":           TokenIn,
	"CONTAINS":     TokenContains,
	"STARTS":       TokenStarts,
	"ENDS":         TokenEnds,
	"IS":           TokenIs,
	"DISTINCT":     TokenDistinct,
	"COUNT":        TokenCount,
	"COLLECT":      TokenCollect,
	"SUM":          TokenSum,
	"AVG":          TokenAvg,
	"MIN":          TokenMin,
	"MAX":          TokenMax,
	"SIZE":         TokenSize,
	"SHORTESTPATH": TokenShortestPath,
	"TRUE":         TokenTrue,
	"FALSE":        TokenFalse,
	"NULL":         TokenNull,
	"DESC":         TokenDesc,
	"ASC":          TokenAsc,
	"UNWIND":       TokenUnwind,
}

// Token is a lexer token.
type Token struct {
	Type    TokenType
	Value   string
	Pos     int
	IntVal  int64
	FloatVal float64
}

// Lexer tokenizes Cypher input.
type Lexer struct {
	input  string
	pos    int
	tokens []Token
}

// Lex tokenizes the input string.
func Lex(input string) ([]Token, error) {
	l := &Lexer{input: input}
	if err := l.tokenize(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *Lexer) tokenize() error {
	for l.pos < len(l.input) {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			break
		}

		ch := l.input[l.pos]

		// Skip comments (// to end of line)
		if ch == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '/' {
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		// Also -- comments (Cypher style)
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			// Check if this is a pattern dash or a comment.
			// In Cypher, -- is used in patterns between () and [] nodes.
			// We treat -- as comment only if not inside a pattern context.
			// Simple heuristic: if next non-space char is not ( [ or >, it's a comment.
			rest := strings.TrimLeft(l.input[l.pos+2:], " \t")
			if len(rest) == 0 || (rest[0] != '(' && rest[0] != '[' && rest[0] != '>') {
				for l.pos < len(l.input) && l.input[l.pos] != '\n' {
					l.pos++
				}
				continue
			}
		}

		switch {
		case ch == '(':
			l.emit(TokenLParen, "(")
		case ch == ')':
			l.emit(TokenRParen, ")")
		case ch == '[':
			l.emit(TokenLBracket, "[")
		case ch == ']':
			l.emit(TokenRBracket, "]")
		case ch == '{':
			l.emit(TokenLBrace, "{")
		case ch == '}':
			l.emit(TokenRBrace, "}")
		case ch == ':':
			l.emit(TokenColon, ":")
		case ch == ',':
			l.emit(TokenComma, ",")
		case ch == '|':
			l.emit(TokenPipe, "|")
		case ch == '*':
			l.emit(TokenStar, "*")
		case ch == '+':
			l.emit(TokenPlus, "+")
		case ch == '/':
			l.emit(TokenSlash, "/")
		case ch == '%':
			l.emit(TokenPercent, "%")
		case ch == '.':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '.' {
				l.pos++
				l.emit(TokenDotDot, "..")
			} else {
				l.emit(TokenDot, ".")
			}
		case ch == '=':
			l.emit(TokenEq, "=")
		case ch == '<':
			if l.pos+1 < len(l.input) {
				next := l.input[l.pos+1]
				if next == '>' {
					l.pos++
					l.emit(TokenNeq, "<>")
				} else if next == '=' {
					l.pos++
					l.emit(TokenLte, "<=")
				} else if next == '-' {
					l.pos++
					l.emit(TokenArrowL, "<-")
				} else {
					l.emit(TokenLt, "<")
				}
			} else {
				l.emit(TokenLt, "<")
			}
		case ch == '>':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
				l.pos++
				l.emit(TokenGte, ">=")
			} else {
				l.emit(TokenGt, ">")
			}
		case ch == '-':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '>' {
				l.pos++
				l.emit(TokenArrowR, "->")
			} else if l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
				l.pos++
				l.emit(TokenDashDash, "--")
			} else {
				// Could be minus or dash. Emit as minus, parser disambiguates.
				l.emit(TokenMinus, "-")
			}
		case ch == '"' || ch == '\'':
			if err := l.lexString(ch); err != nil {
				return err
			}
			continue // lexString advances pos
		case unicode.IsDigit(rune(ch)):
			l.lexNumber()
			continue
		case unicode.IsLetter(rune(ch)) || ch == '_':
			l.lexIdentOrKeyword()
			continue
		default:
			return fmt.Errorf("unexpected character %q at position %d", ch, l.pos)
		}

		l.pos++
	}

	l.tokens = append(l.tokens, Token{Type: TokenEOF, Pos: l.pos})
	return nil
}

func (l *Lexer) emit(tt TokenType, val string) {
	l.tokens = append(l.tokens, Token{Type: tt, Value: val, Pos: l.pos})
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && (l.input[l.pos] == ' ' || l.input[l.pos] == '\t' || l.input[l.pos] == '\n' || l.input[l.pos] == '\r') {
		l.pos++
	}
}

func (l *Lexer) lexString(quote byte) error {
	start := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.pos++
			switch l.input[l.pos] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			case '\'':
				sb.WriteByte('\'')
			default:
				sb.WriteByte(l.input[l.pos])
			}
			l.pos++
			continue
		}
		if ch == quote {
			l.tokens = append(l.tokens, Token{Type: TokenString, Value: sb.String(), Pos: start})
			l.pos++
			return nil
		}
		sb.WriteByte(ch)
		l.pos++
	}
	return fmt.Errorf("unterminated string at position %d", start)
}

func (l *Lexer) lexNumber() {
	start := l.pos
	isFloat := false
	for l.pos < len(l.input) && (unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '.') {
		if l.input[l.pos] == '.' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '.' {
				break // ".." is a separate token
			}
			isFloat = true
		}
		l.pos++
	}

	val := l.input[start:l.pos]
	if isFloat {
		f, _ := strconv.ParseFloat(val, 64)
		l.tokens = append(l.tokens, Token{Type: TokenFloat, Value: val, Pos: start, FloatVal: f})
	} else {
		i, _ := strconv.ParseInt(val, 10, 64)
		l.tokens = append(l.tokens, Token{Type: TokenInt, Value: val, Pos: start, IntVal: i})
	}
}

func (l *Lexer) lexIdentOrKeyword() {
	start := l.pos
	for l.pos < len(l.input) && (unicode.IsLetter(rune(l.input[l.pos])) || unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '_') {
		l.pos++
	}

	val := l.input[start:l.pos]
	upper := strings.ToUpper(val)

	if tt, ok := keywords[upper]; ok {
		l.tokens = append(l.tokens, Token{Type: tt, Value: val, Pos: start})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokenIdent, Value: val, Pos: start})
	}
}
