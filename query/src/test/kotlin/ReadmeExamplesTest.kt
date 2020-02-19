package kquery;

import org.junit.Test
import org.junit.jupiter.api.TestInstance

/**
 * Example source code for book.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadmeExamplesTest {

    val employeeCsv = "src/test/data/employee.csv"

    @Test
    fun sqlExample() {

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
    fun dfExample() {

        // Create a context
        val ctx = ExecutionContext()

        // Construct a query using the DataFrame API
        val df: DataFrame = ctx.csv(employeeCsv)
                .filter(Eq(Column(3), LiteralString("CO")))
                .select(listOf(Column(0), Column(1), Column(2)))

        // Execute the query
        val result = df.collect()
    }

}