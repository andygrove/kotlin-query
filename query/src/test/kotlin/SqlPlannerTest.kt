package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlPlannerTest {

    val employeeCsv = "src/test/data/employee.csv"

    @Test
    fun planSimpleSelect() {

        val ctx = ExecutionContext()
        ctx.register("employee", ctx.csv(employeeCsv))
        val df = ctx.sql("SELECT id FROM employee")

        val expected = "Projection: #0\n" +
                "\tScan: src/test/data/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun planSelectWhere() {

        val ctx = ExecutionContext()
        ctx.register("employee", ctx.csv(employeeCsv))
        val df = ctx.sql("SELECT id FROM employee WHERE state = 'CO'")

        val expected = "Selection: #3 == 'CO'\n" +
                "\tProjection: #0\n" +
                "\t\tScan: src/test/data/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }
}