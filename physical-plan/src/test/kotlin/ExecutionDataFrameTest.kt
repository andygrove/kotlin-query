package io.andygrove.kquery.execution

import io.andygrove.kquery.logical.*

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExecutionDataFrameTest {

    val employeeCsv = "../testdata/employee.csv"

    @Test
    fun `build DataFrame`() {

        val ctx = ExecutionContext()

        val df = ctx.csv(employeeCsv)
            .filter(col("state") eq lit("CO"))
            .select(listOf(col("id"), col("first_name"), col("last_name")))

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan: ../testdata/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    @Test
    fun `multiplier and alias`() {

        val ctx = ExecutionContext()

        val df = ctx.csv(employeeCsv)
                .filter(col("state") eq lit("CO"))
                .select(listOf(
                        col("id"),
                        col("first_name"),
                        col("last_name"),
                        col("salary"),
                        (col("salary") mult lit(0.1)) alias "bonus"))
                .filter(col("bonus") gt lit(1000))

        val expected =
                "Selection: #bonus > 1000\n" +
                "\tProjection: #id, #first_name, #last_name, #salary, #salary * 0.1 as bonus\n" +
                "\t\tSelection: #state = 'CO'\n" +
                "\t\t\tScan: ../testdata/employee.csv; projection=None\n"

        val actual = format(df.logicalPlan())

        println(actual)

        assertEquals(expected, actual)
    }
}
