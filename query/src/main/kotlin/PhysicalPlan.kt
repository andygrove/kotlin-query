package io.andygrove.kquery

import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import java.lang.IllegalStateException

interface PhysicalExpr {
    fun evaluate(input: RecordBatch): FieldVector
}

class ColumnExpr(val column: Column) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        return input.field(column.i)
    }
}


interface PhysicalPlan {
    fun execute(): Iterable<RecordBatch>
}

/**
 * Scan a data source with optional push-down projection.
 */
class ScanExec(val ds: DataSource, val projection: List<Int>) : PhysicalPlan {
    override fun execute(): Iterable<RecordBatch> {
        return ds.scan(projection);
    }
}

class ProjectionExec(val input: PhysicalPlan, val schema: Schema, val expr: List<PhysicalExpr>) : PhysicalPlan {

    override fun execute(): Iterable<RecordBatch> {
        return input.execute().map { batch ->
            val fieldVectors = expr.map { it.evaluate(batch) }
            RecordBatch(schema, VectorSchemaRoot(fieldVectors))
        }
    }
}

class SelectionExec(val input: PhysicalPlan, val expr: PhysicalExpr) : PhysicalPlan {
    override fun execute(): Iterable<RecordBatch> {
        return input.execute().map { batch ->
            when (expr.evaluate(batch)) {
                is BitVector -> {
                    //TODO implement
                    throw IllegalStateException()
                }
                else -> throw IllegalStateException()
            }
        }
    }
}

//class HashAggregateExec(val input: PhysicalPlan, ): PhysicalPlan {
//
//}

