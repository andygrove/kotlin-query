package kquery;

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogicalPlanTest {

    //id,first_name,last_name,state,job_title,salary

    @Test
    fun buildPlanMultiStep() {
        val csv = CsvDataSource("employee.csv")
        val scan = Scan("employee", csv, listOf())
        val filterExpr = Eq(Column(5), LiteralString("CO"))
        val selection = Selection(scan, filterExpr)
        val projectionList = listOf(Column(0), Column(1), Column(2), Column(3), Column(4), Column(5))
        val plan = Projection(selection, projectionList)
        println(format(plan))
    }

    @Test
    fun buildPlanSingleStep() {
        val plan = Projection(
                Selection(
                        Scan("employee", CsvDataSource("employee.csv"), listOf()),
                        Eq(Column(5), LiteralString("CO"))

                ),
                listOf(Column(0), Column(1), Column(2), Column(3), Column(4), Column(5))
        )
        println(format(plan))
    }
}