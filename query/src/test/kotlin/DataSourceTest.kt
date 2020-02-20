package io.andygrove.kquery

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataSourceTest {

    val dir = "src/test/data/"

    @Test
    fun `read csv`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, 1024)
    }

    @Test
    fun `read parquet schema`() {
        val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
        assertEquals("Schema<" +
                "id: Int(32, true), " +
                "bool_col: Bool, " +
                "tinyint_col: Int(32, true), " +
                "smallint_col: Int(32, true), " +
                "int_col: Int(32, true), " +
                "bigint_col: Int(64, true), " +
                "float_col: FloatingPoint(SINGLE), " +
                "double_col: FloatingPoint(DOUBLE), " +
                "date_string_col: Binary, " +
                "string_col: Binary, " +
                "timestamp_col: Binary>", parquet.schema().toString())
    }

}

