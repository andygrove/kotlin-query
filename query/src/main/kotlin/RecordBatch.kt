package kquery

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

class RecordBatch(val schema: Schema, val root: VectorSchemaRoot) {


}