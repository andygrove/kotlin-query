package kquery

import java.lang.IllegalStateException

interface SqlExpr

class Identifier : SqlExpr
class Literal : SqlExpr
class Function : SqlExpr
class BinaryExpr : SqlExpr
class UnaryExpr : SqlExpr
class CastExpr : SqlExpr
class AliasExpr : SqlExpr

interface SqlRelation

class SqlTable : SqlRelation

/**
 * Pratt Top Down Operator Precedence Parser. See https://tdop.github.io/ for paper.
 */
interface PrattParser {

    fun nextPrecedence(): Int
    fun parsePrefix(): SqlExpr?
    fun parseInfix(left: SqlExpr, precendence: Int): SqlExpr

    fun parse(precedence: Int = 0): SqlExpr? {
        var left = parsePrefix()
        while (precedence < nextPrecedence()) {
            left = parseInfix(left!!, nextPrecedence())
        }
        return left
    }
}

interface Token
data class IdentifierToken(val s: String) : Token
data class LiteralStringToken(val s: String) : Token
data class LiteralLongToken(val s: String) : Token
data class KeywordToken(val s: String) : Token
data class OperatorToken(val s: String) : Token

class Tokenizer(val sql: String) {

    val keywords = listOf("SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "GROUP", "ORDER", "BY")

    var i = 0

    fun tokenize(): List<Token> {
        var token = nextToken()
        val list = mutableListOf<Token>()
        while (token != null) {
            list.add(token)
            token = nextToken()
        }
        return list
    }

    private fun nextToken(): Token? {
        if (i >= sql.length) {
            return null
        }

        // skip whitespace
        while (i < sql.length && sql[i].isWhitespace()) {
            i++
        }

        // look for start of token
        if (isIdentifierStart(sql[i])) {
            val start = i
            while (i < sql.length && isIdentifierPart(sql[i])) {
                i++
            }
            val s = sql.substring(start, i)
            if (keywords.contains(s.toUpperCase())) {
                return KeywordToken(s.toUpperCase())
            } else {
                return IdentifierToken(s)
            }

        } else {
            throw IllegalStateException()
        }

    }

    private fun isIdentifierStart(ch: Char) : Boolean {
        return ch.isLetter()
    }

    private fun isIdentifierPart(ch: Char) : Boolean {
        return ch.isLetter() || ch.isDigit() || ch == '_'
    }
}
