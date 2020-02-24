package io.andygrove.kquery.datasource

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.RecordReader
import org.apache.parquet.schema.PrimitiveType

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

    private val reader = ParquetFileReader.open(HadoopInputFile.fromPath(Path(filename), Configuration()))
    val schema = reader.footer.fileMetaData.schema

    override fun iterator(): Iterator<RecordBatch> {
        return ParquetIterator(reader, columns)
    }

    override fun close() {
        reader.close()
    }
}

class ParquetIterator(private val reader: ParquetFileReader, private val projectedColumns: List<Int>) : Iterator<RecordBatch> {

    val schema = reader.footer.fileMetaData.schema

    val arrowSchema = SchemaConverter().fromParquet(schema).arrowSchema

    val projectedArrowSchema = Schema(projectedColumns.map { arrowSchema.fields[it] })

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

        val root = VectorSchemaRoot.create(projectedArrowSchema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = rows

        batch = RecordBatch(projectedArrowSchema, root.fieldVectors.map { ArrowFieldVector(it) })

        //TODO we really want to read directly as columns not rows
        val columnIO = ColumnIOFactory().getColumnIO(schema)
        val recordReader: RecordReader<Group> = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
        for (rowIndex in 0 until rows) {
            val group: Group = recordReader.read()
            for (projectionIndex in 0 until projectedColumns.size) {
                val fieldIndex = projectedColumns[projectionIndex]
                val primitiveTypeName = schema.columns[fieldIndex].primitiveType.primitiveTypeName
                println("column $fieldIndex : $primitiveTypeName")

                if (group.getFieldRepetitionCount(fieldIndex) == 1) {
                    when (primitiveTypeName) {
                        PrimitiveType.PrimitiveTypeName.INT32 -> {
                            (root.fieldVectors[projectionIndex] as IntVector).set(rowIndex, group.getInteger(fieldIndex, 0))
                        }
                        else -> println("unsupported type $primitiveTypeName")
                    }
                }

            }
        }

        return batch
    }


}
