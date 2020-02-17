package kquery

import java.lang.IllegalStateException

interface Token

data class IdentifierToken(val s: String) : Token {
    override fun toString(): String {
        return s
    }
}

data class LiteralStringToken(val s: String) : Token
data class LiteralLongToken(val s: String) : Token
data class KeywordToken(val s: String) : Token
data class OperatorToken(val s: String) : Token

class TokenStream(val tokens: List<Token>) {

    var i = 0

    fun peek() : Token? {
        if (i < tokens.size) {
            return tokens[i]
        } else {
            return null
        }
    }

    fun next() : Token? {
        if (i < tokens.size) {
            return tokens[i++]
        } else {
            return null
        }
    }

    fun consumeKeyword(s: String): Boolean {
        return if (peek() == KeywordToken(s)) {
            i++
            true
        } else {
            false
        }
    }
}

class Tokenizer(val sql: String) {

    val keywords = listOf("SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "GROUP", "ORDER", "BY")

    var i = 0

    fun tokenize(): TokenStream {
        var token = nextToken()
        val list = mutableListOf<Token>()
        while (token != null) {
            list.add(token)
            token = nextToken()
        }
        return TokenStream(list)
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
