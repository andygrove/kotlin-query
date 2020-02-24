package io.andygrove.kquery

import io.andygrove.kquery.datasource.CsvDataSource
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogicalPlanTest {

    val employeeCsv = "src/test/data/employee.csv"

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
        // print the plan
        println(format(plan))
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
        println(format(plan))
    }
}