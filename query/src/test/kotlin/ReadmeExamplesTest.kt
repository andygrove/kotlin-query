package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance

/**
 * Example source code for README in this repo.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadmeExamplesTest {

    val employeeCsv = "src/test/data/employee.csv"

    @Test
    fun `SQL example`() {

        // Create a context
        val ctx = ExecutionContext()

        // Register a CSV data source
        val csv = ctx.csv(employeeCsv)
        ctx.register("employee", csv)

        // Execute a SQL query
        val df = ctx.sql("SELECT id FROM employee")
        val result = df.collect()

        result.forEach {
            println("got batch with schema: ${it.schema}")
            val id = it.field(0)
        }
    }

    @Test
    fun `DataFrame example`() {

        // Create a context
        val ctx = ExecutionContext()

        // Construct a query using the DataFrame API
        val df: DataFrame = ctx.csv(employeeCsv)
                .filter(Eq(col("state"), LiteralString("CO")))
                .select(listOf(col("id"), col("first_name"), col("last_name")))

        // Execute the query
        //TODO: val result = df.collect()
    }

}