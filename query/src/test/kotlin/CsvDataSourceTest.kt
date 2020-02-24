package io.andygrove.kquery

import io.andygrove.kquery.datasource.CsvDataSource
import org.apache.arrow.vector.VarCharVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvDataSourceTest {

    val dir = "src/test/data"

    @Test
    fun `read csv`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, 1024)
        val result = csv.scan(listOf())
        val x = result.asSequence().map {
            val id = it.field(0) as VarCharVector
            println(id.valueCount)
        }
    }

}

