package io.andygrove.kquery

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogicalPlanTest {

    val employeeCsv = "src/test/data/employee.csv"
    //id,first_name,last_name,state,job_title,salary

    @Test
    fun buildPlanMultiStep() {
        // create a plan to represent the data source
        val csv = CsvDataSource(employeeCsv, 10)
        // create a plan to represent the scan of the data source (FROM)
        val scan = Scan("employee", csv, listOf())
        // create a plan to represent the selection (WHERE)
        val filterExpr = Eq(Column(3), LiteralString("CO"))
        val selection = Selection(scan, filterExpr)
        // create a plan to represent the projection (SELECT)
        val projectionList = listOf(Column(0), Column(1), Column(2), Column(3), Column(4), Column(5))
        val plan = Projection(selection, projectionList)
        // print the plan
        println(format(plan))
    }

    @Test
    fun buildPlanSingleStep() {
        val plan = Projection(
                Selection(
                        Scan("employee", CsvDataSource(employeeCsv, 10), listOf()),
                        Eq(Column(3), LiteralString("CO"))
                ),
                listOf(Column(0), Column(1), Column(2), Column(3), Column(4), Column(5))
        )
        println(format(plan))
    }
}