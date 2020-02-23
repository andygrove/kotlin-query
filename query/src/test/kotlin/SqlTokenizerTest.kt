package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

    @Test
    fun `tokenize simple SELECT`() {
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
    fun `tokenize SELECT with WHERE`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("a"),
                PunctuationToken(","),
                IdentifierToken("b"),
                KeywordToken("FROM"),
                IdentifierToken("employee"),
                KeywordToken("WHERE"),
                IdentifierToken("state"),
                OperatorToken("="),
                LiteralStringToken("CO")
        )
        val actual = tokenize("SELECT a, b FROM employee WHERE state = 'CO'")
        assertEquals(expected, actual)

        /*
        expected: <[KeywordToken(s=SELECT), a, PunctuationToken(s=,), b, KeywordToken(s=FROM), employee, KeywordToken(s=WHERE), state, OperatorToken(s==), LiteralStringToken(s=CO)]>
        but was:  <[KeywordToken(s=SELECT), a, PunctuationToken(s=,), b, KeywordToken(s=FROM), employee, KeywordToken(s=WHERE), state, OperatorToken(s==), LiteralStringToken(s=CO), LiteralStringToken(s=)]>
	at org.junit.jupiter.api.AssertionUtils.fail(AssertionUtils.java:55)
	at org.junit.jupiter.api.AssertionUtils.failNotEqual(AssertionUtils.java:62)
	at org.junit.jupiter.api.AssertEquals.assertEquals(AssertEquals.java:182)
	at org.junit.jupiter.api.Assertions.assertEquals(Assertions.java:1135)
         */
    }

    private fun tokenize(sql: String) : List<Token> {
        return SqlTokenizer(sql).tokenize().tokens
    }
}