package io.andygrove.kquery.datasource

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

/**
 * Batch of data organized in columns.
 */
class RecordBatch(val schema: Schema, val root: VectorSchemaRoot) {

    /** Access one column by index */
    fun field(i: Int): FieldVector {
        return root.getVector(i)
    }

    /** Useful for testing */
    fun toCSV() : String {
        val b = StringBuilder()
        val columnCount = schema.fields.size
        val rowCount = root.fieldVectors.first().valueCount

        (0 until rowCount).forEach { rowIndex ->
            (0 until columnCount).forEach { columnIndex ->
                if (columnIndex > 0) {
                    b.append(",")
                }
                val v = root.fieldVectors[columnIndex]
                when (v) {
                    is VarCharVector -> {
                        if (v.isNull(rowIndex)) {
                            b.append("null")
                        } else {
                            b.append(String(v.get(rowIndex)))
                        }
                    }
                    else -> TODO()
                }
            }
            b.append("\n")
        }
        return b.toString()
    }
}


