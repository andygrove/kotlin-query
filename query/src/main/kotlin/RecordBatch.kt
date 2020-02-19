package io.andygrove.kquery

import org.apache.arrow.vector.FieldVector
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


}