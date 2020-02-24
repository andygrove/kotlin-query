package io.andygrove.kquery.sql

import java.util.logging.Logger

class TokenStream(val tokens: List<Token>) {

    private val logger = Logger.getLogger(TokenStream::class.simpleName)

    var i = 0

    fun peek(): Token? {
        if (i < tokens.size) {
            return tokens[i]
        } else {
            return null
        }
    }

    fun next(): Token? {
        if (i < tokens.size) {
            return tokens[i++]
        } else {
            return null
        }
    }

    fun consumeKeyword(s: String): Boolean {
        val peek = peek()
        logger.fine("consumeKeyword('$s') next token is $peek")
        return if (peek == KeywordToken(s)) {
            i++
            logger.fine("consumeKeyword() returning true")
            true
        } else {
            logger.fine("consumeKeyword() returning false")
            false
        }
    }
}