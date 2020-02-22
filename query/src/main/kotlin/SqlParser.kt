package io.andygrove.kquery

import java.sql.SQLException

/** SQL Expression */
interface SqlExpr

/** Simple SQL identifier such as a table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
    override fun toString() = id
}

/** Binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
    override fun toString(): String = "$l $op $r"
}

/** SQL literal string */
data class SqlString(val value: String) : SqlExpr {
    override fun toString() = "'$value'"
}

/** SQL literal long */
data class SqlLong(val value: Long) : SqlExpr {
    override fun toString() = "$value"
}

//TODO: support other expression types

//data class Function() : SqlExpr
//data class UnaryExpr() : SqlExpr
//data class CastExpr() : SqlExpr
//data class AliasExpr() : SqlExpr

interface SqlRelation : SqlExpr

//TODO: GROUP BY, ORDER BY, LIMIT, OFFSET
data class SqlSelect(val projection: List<SqlExpr>, val selection: SqlExpr?, val tableName: String) : SqlRelation

/**
 * Pratt Top Down Operator Precedence Parser. See https://tdop.github.io/ for paper.
 */
interface PrattParser {

    /** Parse an expression */
    fun parse(precedence: Int = 0): SqlExpr? {
        var expr = parsePrefix() ?: return null
        while ( precedence < nextPrecedence()) {
            expr = parseInfix(expr, nextPrecedence())
        }
        return expr
    }

    /** Get the precedence of the next token */
    fun nextPrecedence(): Int

    /** Parse the next prefix expression */
    fun parsePrefix(): SqlExpr?

    /** Parse the next infix expression */
    fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr

}

class SqlParser(val tokens: TokenStream) : PrattParser {

    override fun nextPrecedence(): Int {
        val token = tokens.peek() ?: return 0
        val precedence = when (token) {
            is KeywordToken -> {
                when (token.s) {
                    "OR" -> 20
                    "AND" -> 30
                    else -> 0
                }
            }
            is OperatorToken -> {
                when (token.s) {
                    "<", "<=", "=", "!=", ">=", ">" -> 40
                    "+", "-" -> 50
                    "*", "/" -> 60
                    else -> 0
                }
            }
            else -> 0
        }
        println("nextPrecedence($token) returning $precedence")
        return precedence
    }

    override fun parsePrefix(): SqlExpr? {
        println("parsePrefix() next token = ${tokens.peek()}")
        val token = tokens.next() ?: return null
        val expr = when (token) {
            is KeywordToken -> {
              when (token.s) {
                  "SELECT" -> parseSelect()
                  else -> throw IllegalStateException("Unexpected keyword ${token.s}")
              }
            }
            is IdentifierToken -> SqlIdentifier(token.s)
            is LiteralStringToken -> SqlString(token.s)
            is LiteralLongToken -> SqlLong(token.s.toLong())
            else -> throw IllegalStateException("Unexpected token $token")
        }
        println("parsePrefix() returning $expr")
        return expr
    }

    override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
        println("parseInfix() next token = ${tokens.peek()}")
        val token = tokens.peek()
        val expr = when (token) {
            is OperatorToken -> {
                tokens.next()
                SqlBinaryExpr(left, token.s, parse(precedence) ?: throw SQLException("Error parsing infix"))
            }
            else -> throw IllegalStateException("Unexpected infix token $token")
        }
        println("parseInfix() returning $expr")
        return expr
    }

    private fun parseSelect() : SqlSelect {
        val projection = parseExprList()
        if (tokens.consumeKeyword("FROM")) {
            val table = parseExpr() as SqlIdentifier
            if (tokens.consumeKeyword("WHERE")) {
                return SqlSelect(projection, parseExpr(), table.id)
            } else {
                return SqlSelect(projection, null, table.id)
            }
        } else {
            throw IllegalStateException("Expected FROM keyword")
        }
    }

    private fun parseExprList() : List<SqlExpr> {
        println("parseExprList()")
        val list = mutableListOf<SqlExpr>()
        var expr = parseExpr()
        while (expr != null) {
            //println("parseExprList parsed $expr")
            list.add(expr)
            if (tokens.peek() == PunctuationToken(",")) {
                tokens.next()
            } else {
                break
            }
            expr = parseExpr()
        }
        println("parseExprList() returning $list")
        return list
    }

    private fun parseExpr() = parse(0)


}
