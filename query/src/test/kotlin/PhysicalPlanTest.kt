package io.andygrove.kquery

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhysicalPlanTest {

    val dir = "src/test/data"

    @Test
    fun `employees in CO`() {
        // Create a context
        val ctx = ExecutionContext()

        // Construct a query using the DataFrame API
        val df = ctx.csv(File(dir, "employee.csv").absolutePath)
                .filter(col("state") eq lit("CO"))
                .select(listOf(col("id"), col("first_name"), col("last_name")))

        val batches = df.collect().asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals(
                "2,Gregg,Langford\n" +
                "3,John,Travis\n", batch.toCSV())
    }

    @Test
    fun `employees in CA`() {
        // Create a context
        val ctx = ExecutionContext()

        // Construct a query using the DataFrame API
        val df = ctx.csv(File(dir, "employee.csv").absolutePath)
                .filter(col("state") eq lit("CA"))
                .select(listOf(col("id"), col("first_name"), col("last_name")))

        val batches = df.collect().asSequence().toList()
        assertEquals(1, batches.size)

        val batch = batches.first()
        assertEquals("1,Bill,Hopkins\n"
                , batch.toCSV())
    }

}