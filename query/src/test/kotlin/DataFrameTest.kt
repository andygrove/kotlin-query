package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    val employeeCsv = "src/test/data/employee.csv"

    @Test
    fun `build DataFrame`() {

        val ctx = ExecutionContext()

        val df = ctx.csv(employeeCsv)
            .filter(col("state") eq "CO")
            .select(listOf(col("id"), col("first_name"), col("last_name")))

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan: src/test/data/employee.csv; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }
}