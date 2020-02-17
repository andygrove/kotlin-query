package kquery

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

class RecordBatch(val schema: Schema, val root: VectorSchemaRoot) {

    fun field(i: Int): FieldVector {
        return root.getVector(i)
    }


}