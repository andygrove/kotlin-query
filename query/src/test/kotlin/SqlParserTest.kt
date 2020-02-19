package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlParserTest {

    @Test
    fun `parse simple SELECT`() {
        val tokens = tokenize("SELECT a FROM b")
        val ast = SqlParser(tokens).parse()
        println(ast)

        val select = ast as SqlSelect
        assertEquals(listOf(Identifier("a")), select.projection)
        assertEquals("b", select.tableName)
    }

    private fun tokenize(sql: String) : TokenStream {
        return Tokenizer(sql).tokenize()
    }
}