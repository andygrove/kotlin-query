package kquery;

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

/**
 * Example source code for book.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExamplesTest {

    val employeeCsv = "src/test/data/employee.csv"

    @Test
    fun simpleSelect() {

        val ctx = ExecutionContext()

        val csv = ctx.csv(employeeCsv)

        ctx.register("employee", csv)

        val df = ctx.sql("SELECT id FROM employee")

        val batches = df.collect()

        batches.forEach {
            println("got batch with schema: ${it.schema}")
            val id = it.field(0)
        }
    }

}