package io.andygrove.kquery

import io.andygrove.kquery.datasource.ParquetDataSource
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParquetDataSourceTest {

    val dir = "src/test/data/"

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

    @Test
    fun `read parquet file`() {
        val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
        val it = parquet.scan(listOf(0)).iterator()
        assertTrue(it.hasNext())

        val batch = it.next()
        assertEquals(1, batch.schema.fields.size)
        assertEquals(8, batch.field(0).valueCount)

        val id = batch.field(0) as IntVector
        val values = (0..id.valueCount).map {
            if (id.isNull(it)) {
                "null"
            } else {
                id.get(it).toString()
            }
        }
        assertEquals("4,5,6,7,2,3,0,1,null", values.joinToString(","))

        assertFalse(it.hasNext())
    }
}

