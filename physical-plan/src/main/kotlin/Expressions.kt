package io.andygrove.kquery.physical

import io.andygrove.kquery.datasource.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import java.util.*

/**
 * Physical representation of an expression.
 */
interface PhysicalExpr {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): ColumnVector
}

class ColumnPExpr(val i: Int) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }
}


abstract class ComparisonPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return compare(ll, rr)
    }

    abstract fun compare(l: ColumnVector, r: ColumnVector) : ColumnVector
}

class EqExpr(l: PhysicalExpr, r: PhysicalExpr): ComparisonPExpr(l,r) {

    override fun compare(l: ColumnVector, r: ColumnVector): ColumnVector {
        assert(l.size() == r.size())
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        (0 until l.size()).forEach {
            if (eq(l.getValue(it), r.getValue(it))) {
                v.set(it, 1)
            } else {
                v.set(it, 0)
            }
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    private fun eq(l: Any?, r: Any?) : Boolean {
        //TODO
        return if (l is ByteArray) {
            if (r is ByteArray) {
                Arrays.equals(l, r)
            } else if (r is String) {
                Arrays.equals(l, r.toByteArray())
            } else {
                TODO()
            }
        } else {
            l == r
        }
    }
}

abstract class BinaryPExpr(val l: PhysicalExpr, val r: PhysicalExpr) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return evaluate(ll, rr)
    }

    abstract fun evaluate(l: ColumnVector, r: ColumnVector) : ColumnVector
}

class MultExpr(l: PhysicalExpr, r: PhysicalExpr): BinaryPExpr(l,r) {

    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {

        assert(l.size() == r.size())
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        //TODO make this generic so it supports all numeric types .. this is hard coded for the one test that uses it

        TODO()

//        when (l) {
//            is BigIntVector -> {
//                val rr = r as Float8Vector
//                (0 until l.valueCount).forEach {
//                    val leftValue = l.get(it)
//                    val rightValue = rr.get(it)
//                    //println("${String(leftValue)} == ${String(rightValue)} ?")
//                    v.set(it, leftValue.toDouble() * rightValue)
//                }
//            }
//            else -> TODO()
//        }
//        v.valueCount = l.valueCount
//        return ArrowFieldVector(v)
    }
}

class LiteralLongPExpr(val value: Long) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value, input.rowCount())
    }
}

class LiteralDoublePExpr(val value: Double) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value, input.rowCount())
    }
}

class LiteralStringPExpr(val value: String) : PhysicalExpr {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value.toByteArray(), input.rowCount())
    }
}
