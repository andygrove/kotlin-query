package io.andygrove.kquery.logical

import io.andygrove.kquery.datasource.CsvDataSource
import org.apache.arrow.vector.types.pojo.ArrowType
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogicalPlanTest {

    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `build logicalPlan manually`() {
        // create a plan to represent the data source
        val csv = CsvDataSource(employeeCsv, 10)
        // create a plan to represent the scan of the data source (FROM)
        val scan = Scan("employee", csv, listOf())
        // create a plan to represent the selection (WHERE)
        val filterExpr = Eq(col("state"), LiteralString("CO"))
        val selection = Selection(scan, filterExpr)
        // create a plan to represent the projection (SELECT)
        val projectionList = listOf(col("id"), col("first_name"), col("last_name"))
        val plan = Projection(selection, projectionList)

        assertEquals(
                "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan: employee; projection=None\n", format(plan))
    }

    @Test
    fun `build logicalPlan nested`() {
        val plan = Projection(
                Selection(
                        Scan("employee", CsvDataSource(employeeCsv, 10), listOf()),
                        Eq(col("state"), LiteralString("CO"))
                ),
                listOf(col("id"), col("first_name"), col("last_name"))
        )

        assertEquals(
                "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                "\t\tScan: employee; projection=None\n", format(plan))
    }

    @Test
    fun `build aggregate plan`() {
        // create a plan to represent the data source
        val csv = CsvDataSource(employeeCsv, 10)

        // create a plan to represent the scan of the data source (FROM)
        val scan = Scan("employee", csv, listOf())

        val groupExpr = listOf(col("state"))
        val aggregateExpr = listOf(Max(cast(col("salary"), ArrowType.Int(32, true))))
        val plan = Aggregate(scan, groupExpr, aggregateExpr)

        assertEquals(
                "Aggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS Int(32, true)))]\n" +
                        "\tScan: employee; projection=None\n", format(plan))

    }
}