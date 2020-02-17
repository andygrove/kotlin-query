package kquery;

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTest {

    @Test
    fun test() {
        val expected = listOf(KeywordToken("SELECT"), IdentifierToken("a"), KeywordToken("FROM"), IdentifierToken("b"))
        val actual = parse("SELECT a FROM b")
        assertEquals(expected, actual)
    }

    private fun parse(sql: String) : List<Token> {
        return Tokenizer(sql).tokenize()
    }
}