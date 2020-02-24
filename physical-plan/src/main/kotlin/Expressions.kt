package io.andygrove.kquery.execution

import io.andygrove.kquery.datasource.DataSource
import io.andygrove.kquery.datasource.RecordBatch
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.Schema
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

abstract class BinaryPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): FieldVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return evaluate(ll, rr)
    }

    abstract fun evaluate(l: FieldVector, r: FieldVector) : FieldVector
}

class MultExpr(l: PhysicalExpr, r: PhysicalExpr): BinaryPExpr(l,r) {

    override fun evaluate(l: FieldVector, r: FieldVector): FieldVector {

        assert(l.valueCount == r.valueCount)
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        //TODO make this generic so it supports all numeric types .. this is hard coded for the one test that uses it

        when (l) {
            is BigIntVector -> {
                val rr = r as Float8Vector
                (0 until l.valueCount).forEach {
                    val leftValue = l.get(it)
                    val rightValue = rr.get(it)
                    //println("${String(leftValue)} == ${String(rightValue)} ?")
                    v.set(it, leftValue.toDouble() * rightValue)
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
