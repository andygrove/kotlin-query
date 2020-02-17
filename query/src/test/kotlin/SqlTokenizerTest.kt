package kquery;

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

    @Test
    fun tokenizeSimpleSelect() {
        val expected = listOf(KeywordToken("SELECT"), IdentifierToken("a"), KeywordToken("FROM"), IdentifierToken("b"))
        val actual = tokenize("SELECT a FROM b")
        assertEquals(expected, actual)
    }

    private fun tokenize(sql: String) : List<Token> {
        return Tokenizer(sql).tokenize().tokens
    }
}