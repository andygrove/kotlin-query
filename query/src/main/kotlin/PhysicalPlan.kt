package io.andygrove.kquery

import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import java.lang.IllegalStateException

/**
 * Physical representation of an expression.
 */
interface PhysicalExpr {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): FieldVector
}

class ColumnExpr(val i: Int) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        return input.field(i)
    }
}

class EqExpr(val l: PhysicalExpr, val r: PhysicalExpr): PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

/**
 * A physical plan represents an executable piece of code that will produce data.
 */
interface PhysicalPlan {

    /**
     * Execute a physical plan and produce a series of record batches.
     */
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

/**
 * Execute a projection.
 */
class ProjectionExec(val input: PhysicalPlan, val schema: Schema, val expr: List<PhysicalExpr>) : PhysicalPlan {

    override fun execute(): Iterable<RecordBatch> {
        return input.execute().map { batch ->
            val fieldVectors = expr.map { it.evaluate(batch) }
            RecordBatch(schema, VectorSchemaRoot(fieldVectors))
        }
    }
}

/**
 * Execute a selection.
 */
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

