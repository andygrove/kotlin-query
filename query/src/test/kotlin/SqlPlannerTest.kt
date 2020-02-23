package io.andygrove.kquery

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlPlannerTest {

    val employeeCsv = "src/test/data/employee.csv"

    @Test
    fun `simple SELECT`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT id FROM employee")

        val expected =
            "Projection: #id\n" +
            "\tScan: src/test/data/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `SELECT with WHERE`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT id FROM employee WHERE state = 'CO'")

        val expected =
            "Selection: #state = 'CO'\n" +
            "\tProjection: #id\n" +
            "\t\tScan: src/test/data/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `SELECT with aliased binary expression`() {

        val ctx = createContext()

        val df = ctx.sql("SELECT salary * 0.1 AS bonus FROM employee")

        val expected =
                "Projection: #salary * 0.1 as bonus\n" +
                "\tScan: src/test/data/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    private fun createContext() : ExecutionContext {
        val ctx = ExecutionContext()
        ctx.register("employee", ctx.csv(employeeCsv))
        return ctx
    }
}