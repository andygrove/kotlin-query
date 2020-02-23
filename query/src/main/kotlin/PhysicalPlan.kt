package io.andygrove.kquery

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.Schema
import java.lang.IllegalStateException
import java.util.*

/**
 * Physical representation of an expression.
 */
interface PhysicalExpr {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): FieldVector
}

class ColumnPExpr(val i: Int) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        return input.field(i)
    }
}

abstract class ComparisonPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return compare(ll, rr)
    }

    abstract fun compare(l: FieldVector, r: FieldVector) : FieldVector
}

class EqExpr(l: PhysicalExpr, r: PhysicalExpr): ComparisonPExpr(l,r) {
    override fun compare(l: FieldVector, r: FieldVector): FieldVector {
        assert(l.valueCount == r.valueCount)
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        when (l) {
            is VarCharVector -> {
                val rr = r as VarCharVector
                (0 until l.valueCount).forEach {
                    val leftValue: ByteArray = l.get(it)
                    val rightValue: ByteArray = rr.get(it)
                    //println("${String(leftValue)} == ${String(rightValue)} ?")
                    if (Arrays.equals(leftValue, rightValue)) {
                        v.set(it, 1)
                    } else {
                        v.set(it, 0)
                    }
                }
            }
            else -> TODO()
        }
        v.valueCount = l.valueCount
        return v
    }
}

class LiteralLongPExpr(val value: Long) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        //TODO this is dumb and we should be able to specify literals more efficiently
        val size = input.field(0).valueCount
        val v = BigIntVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        v.valueCount = size
        0.rangeTo(size).forEach { v.set(it, value) }
        return v
    }
}

class LiteralDoublePExpr(val value: Double) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        //TODO this is dumb and we should be able to specify literals more efficiently
        val size = input.field(0).valueCount
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        v.valueCount = size
        0.rangeTo(size).forEach { v.set(it, value) } // TODO need Long not Int
        return v
    }
}

class LiteralStringPExpr(val value: String) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        //TODO this is dumb and we should be able to specify literals more efficiently
        val size = input.field(0).valueCount
        val v = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        v.valueCount = size
        0.rangeTo(size).forEach { v.set(it, value.toByteArray()) } // TODO need Long not Int
        return v
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

            println("projection input:\n${batch.toCSV()}")

            val fieldVectors = expr.map { it.evaluate(batch) }
            val batch = RecordBatch(schema, VectorSchemaRoot(fieldVectors))

            println("projection output:\n${batch.toCSV()}")

            batch
        }
    }
}

/**
 * Execute a selection.
 */
class SelectionExec(val input: PhysicalPlan, val expr: PhysicalExpr) : PhysicalPlan {
    override fun execute(): Iterable<RecordBatch> {
        val input = input.execute()
        return input.map { batch ->

            println("selection input:\n${batch.toCSV()}")

            val result = expr.evaluate(batch) as BitVector
            val schema = batch.schema
            val columnCount = batch.schema.fields.size
            val rowCount = batch.field(0).valueCount
            val filteredFields = (0 until columnCount).map { filter(batch.field(it), result) }
            val filteredBatch = VectorSchemaRoot(schema, filteredFields.toList(), rowCount)
            val batch = RecordBatch(schema, filteredBatch)

            println("selection output:\n${batch.toCSV()}")

            batch
        }
    }

    private fun filter(v: FieldVector, selection: BitVector) : FieldVector {
        return when (v) {
            is VarCharVector -> {
                val size = v.valueCount
                val filteredVector = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
                filteredVector.allocateNew()
                var count = 0
                (0 until size)
                    .forEach {
                        if (selection.get(it) == 1) {
                            filteredVector.set(count, v.get(it))
                            count++
                        }
                    }
                filteredVector.valueCount = count
                return filteredVector
            }
            else -> TODO()
        }
    }
}

