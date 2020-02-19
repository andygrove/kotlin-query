package io.andygrove.kquery

import com.github.doyaaaaaken.kotlincsv.client.CsvReader
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema

import java.io.File
import java.lang.UnsupportedOperationException

interface DataSource {
    fun schema(): Schema
    fun scan(columns: List<Int>): Iterable<RecordBatch>
}

/**
 * Simple CSV data source that assumes that the first line contains field names and that all values are strings.
 */
class CsvDataSource(filename: String, val batchSize: Int) : DataSource {

    val rows: List<List<String>> = CsvReader().readAll(File(filename))

    val schema = Schema(rows[0].map { Field.nullable(it, ArrowType.Utf8()) })

    override fun schema(): Schema {
        return schema
    }

    override fun scan(columns: List<Int>): Iterable<RecordBatch> {
        return rows.drop(1).chunked(batchSize).map { rows ->
            val root = VectorSchemaRoot.create(schema, RootAllocator(Long.MAX_VALUE))
            root.allocateNew()
            root.rowCount = rows.size

            root.fieldVectors.withIndex().forEach { field ->
                field.value.valueCount = rows.size
                when (field.value) {
                    is VarCharVector -> rows.withIndex().forEach { row ->
                        val value = row.value.toString()
                        (field.value as VarCharVector).set(row.index, value.toByteArray())
                    }
                    else -> throw UnsupportedOperationException()
                }
            }
            RecordBatch(schema, root)
        }.asIterable()
    }
}