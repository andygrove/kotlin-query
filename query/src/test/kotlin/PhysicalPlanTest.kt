package io.andygrove.kquery

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhysicalPlanTest {

    val employeeCsv = "src/test/data/employee.csv"
    //id,first_name,last_name,state,job_title,salary

    @Test
    @Ignore
    fun executeProjection() {
        // create a plan to represent the data source
        val csv = CsvDataSource(employeeCsv, 10)
        // create a plan to represent the scan of the data source (FROM)
        val scan = Scan("employee", csv, listOf())
        // create a plan to represent the selection (WHERE)
        val filterExpr = Eq(col("state"), LiteralString("CO"))
        val selection = Selection(scan, filterExpr)
        // create a plan to represent the projection (SELECT)
        val projectionList = listOf(col("id"), col("first_name"), col("last_name"))
        val projection = Projection(selection, projectionList)
        // print the plan
        println(format(projection))

        val physicalPlan = QueryPlanner().createPhysicalPlan(projection)

        physicalPlan.execute().forEach { batch ->
            println("got batch with ${batch.schema.fields.size} cols and ${batch.field(0).valueCount} rows")
        }
    }


}