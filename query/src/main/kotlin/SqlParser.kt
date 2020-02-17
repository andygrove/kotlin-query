package kquery

import java.lang.IllegalStateException

interface SqlExpr

/** Simple identifier such as a table or column name */
data class Identifier(val id: String) : SqlExpr {
    override fun toString(): String {
        return id
    }
}

//data class Literal() : SqlExpr
//data class Function() : SqlExpr
//data class BinaryExpr() : SqlExpr
//data class UnaryExpr() : SqlExpr
//data class CastExpr() : SqlExpr
//data class AliasExpr() : SqlExpr

interface SqlRelation : SqlExpr

data class SqlSelect(val projection: List<SqlExpr>) : SqlRelation

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

class SqlParser(val tokens: TokenStream) : PrattParser {

    override fun nextPrecedence(): Int {
        val token = tokens.peek()
        return when (token) {
            is KeywordToken -> {
                when (token.toString()) {
                    "OR" -> 20
                    "AND" -> 30
                    else -> 0
                }
            }
            is OperatorToken -> {
                when (token.toString()) {
                    "<", "<=", "=", "!=", ">=", ">" -> 40
                    "+", "-" -> 50
                    "*", "/" -> 60
                    else -> 0
                }
            }
            else -> 0
        }
    }

    override fun parsePrefix(): SqlExpr? {
        val token = tokens.next()

        if (token == null) {
            return null
        } else if (token == KeywordToken("SELECT")) {
            return parseSelect()
        } else {
            throw IllegalStateException("Unexpected token $token")
        }
    }

    override fun parseInfix(left: SqlExpr, precendence: Int): SqlExpr {

        //TODO binary expressions

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun parseSelect() : SqlSelect {
        val projection = parseExprList()
        return SqlSelect(projection)
    }

    private fun parseExprList() : List<SqlExpr> {
        val list = mutableListOf<SqlExpr>()
        var expr = parseExpr()
        while (expr != null) {
            list.add(expr)
            if (tokens.peek() == OperatorToken(",")) {
                tokens.next()
            } else {
                break
            }
            expr = parseExpr()
        }
        return list
    }

    /** Parse a single SQL expression */
    private fun parseExpr() : SqlExpr? {
        var token = tokens.peek()
        when (token) {
            is KeywordToken -> return null
            is IdentifierToken -> return Identifier(token.toString())
            else -> TODO()
        }
    }
}