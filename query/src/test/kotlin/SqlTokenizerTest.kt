package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

    @Test
    fun tokenizeSimpleSelect() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("a"),
                PunctuationToken(","),
                IdentifierToken("b"),
                KeywordToken("FROM"),
                IdentifierToken("employee")
        )
        val actual = tokenize("SELECT a, b FROM employee")
        assertEquals(expected, actual)
    }

    @Test
    fun tokenizeSelectWithWhere() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("a"),
                PunctuationToken(","),
                IdentifierToken("b"),
                KeywordToken("FROM"),
                IdentifierToken("employee"),
                KeywordToken("WHERE"),
                IdentifierToken("a"),
                OperatorToken("="),
                LiteralLongToken("5")
        )
        val actual = tokenize("SELECT a, b FROM employee WHERE a = 5")
        assertEquals(expected, actual)
    }

    private fun tokenize(sql: String) : List<Token> {
        return Tokenizer(sql).tokenize().tokens
    }
}