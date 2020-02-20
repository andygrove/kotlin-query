package io.andygrove.kquery

import com.github.doyaaaaaken.kotlincsv.client.CsvReader

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter

import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.RecordReader
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type

import java.io.File
import java.io.IOException
import java.lang.IllegalStateException
import java.util.*


interface DataSource {

    /** Return the schema for the underlying data source */
    fun schema(): Schema

    /** Scan the data source, selecting the specified columns */
    fun scan(columns: List<Int>): Iterable<RecordBatch>
}

/**
 * Simple CSV data source that assumes that the first line contains field names and that all values are strings.
 *
 * Note that this implementation loads the entire CSV file into memory so is not scalable. I plan on implementing
 * a streaming version later on.
 */
class CsvDataSource(filename: String, private val batchSize: Int) : DataSource {

    private val rows: List<List<String>> = CsvReader().readAll(File(filename))

    private val schema = Schema(rows[0].map { Field.nullable(it, ArrowType.Utf8()) })

    override fun schema(): Schema {
        return schema
    }

    override fun scan(columns: List<Int>): Iterable<RecordBatch> {
        return rows.asSequence().drop(1).chunked(batchSize).map { rows ->
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

class ParquetDataSource(private val filename: String) : DataSource {

    override fun schema(): Schema {
        return ParquetScan(filename, listOf()).use {
            SchemaConverter().fromParquet(it.schema).arrowSchema
        }
    }

    override fun scan(columns: List<Int>): Iterable<RecordBatch> {
        return ParquetScan(filename, columns)
    }

}

/**
 * Based on blog post at https://www.arm64.ca/post/reading-parquet-files-java/
 */
class ParquetScan(filename: String, private val columns: List<Int>) : AutoCloseable, Iterable<RecordBatch> {

    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(Path(filename), Configuration()))
    val schema = reader.footer.fileMetaData.schema

    override fun iterator(): Iterator<RecordBatch> {
        return ParquetIterator(reader)
    }

    override fun close() {
        reader.close()
    }
}

class ParquetIterator(private val reader: ParquetFileReader) : Iterator<RecordBatch> {

    val schema = reader.footer.fileMetaData.schema

    val arrowSchema = SchemaConverter().fromParquet(schema).arrowSchema

    var batch: RecordBatch? = null

    override fun hasNext(): Boolean {
        batch = nextBatch()
        return batch != null
    }

    override fun next(): RecordBatch {
        val next = batch
        batch = null
        return next!!
    }

    private fun nextBatch() : RecordBatch? {
        val pages = reader.readNextRowGroup()
        if (pages == null) {
            return null
        }

        if (pages.rowCount > Integer.MAX_VALUE) {
            throw IllegalStateException()
        }

        val rows = pages.rowCount.toInt()
        println("Reading $rows rows")

        val root = VectorSchemaRoot.create(arrowSchema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = rows

        batch = RecordBatch(arrowSchema, root)

        //TODO we really want to read directly as columns not rows
        val columnIO = ColumnIOFactory().getColumnIO(schema)
        val recordReader: RecordReader<Group> = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
        for (i in 0 until rows) {
            val group: Group = recordReader.read()
            for (j in 0 until arrowSchema.fields.size) {
                val primitiveTypeName = schema.columns[j].primitiveType.primitiveTypeName
                when (primitiveTypeName) {
                    PrimitiveType.PrimitiveTypeName.INT32 -> {
                        group.getInteger(i,j)
                    }
                    else -> println("unsupported type $primitiveTypeName")
                }
            }
        }

        return batch
    }


}
