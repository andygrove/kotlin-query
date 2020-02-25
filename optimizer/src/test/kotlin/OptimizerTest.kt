package io.andygrove.kquery.logical

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.optimizer.PredicatePushDownRule
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OptimizerTest {

    @Test
    fun `predicate push down`() {

        val df = csv()
                .select(listOf(col("id"), col("first_name"), col("last_name")))

        val rule = PredicatePushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())

        val expected =
                "Projection: #id, #first_name, #last_name\n" +
                "\tScan: employee; projection=[first_name, id, last_name]\n"

        assertEquals(expected, format(optimizedPlan))
    }

    private fun csv() : DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, 1024), listOf()))
    }
}