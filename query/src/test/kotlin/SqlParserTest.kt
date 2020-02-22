package io.andygrove.kquery

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlParserTest {

    @Test
    fun `1 + 2 * 3`() {
        val tokens = tokenize("1 + 2 * 3")
        val ast = SqlParser(tokens).parse()
    println(ast)
        val expected = SqlBinaryExpr(SqlLong(1),
                "+",
                SqlBinaryExpr(SqlLong(2), "*", SqlLong(3))
        )
        assertEquals(expected, ast)
    }

    @Test
    @Ignore
    fun `1 * 2 + 3`() {
        val tokens = tokenize("1 * 2 + 3")
        val ast = SqlParser(tokens).parse()
        println(ast)
        val expected = SqlBinaryExpr(
                SqlBinaryExpr(SqlLong(1), "*", SqlLong(2)),
                "+",
                SqlLong(3)
        )
        assertEquals(expected, ast)
    }

    @Test
    @Ignore
    fun `parse simple SELECT`() {
        val tokens = tokenize("SELECT id, first_name, last_name FROM employee")
        val ast = SqlParser(tokens).parse()
        println(ast)

        val select = ast as SqlSelect
        assertEquals(listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")), select.projection)
        assertEquals("employee", select.tableName)
    }

    @Test
    @Ignore
    fun `parse SELECT with WHERE`() {
        val tokens = tokenize("SELECT id, first_name, last_name FROM employee WHERE state = 'CO'")
        val ast = SqlParser(tokens).parse()
        println(ast)

        val select = ast as SqlSelect
        assertEquals(listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")), select.projection)
        assertEquals(SqlBinaryExpr(SqlIdentifier("state"), "=", SqlString("CO")), select.selection)
        assertEquals("employee", select.tableName)
    }

    private fun tokenize(sql: String) : TokenStream {
        return Tokenizer(sql).tokenize()
    }
}