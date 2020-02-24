package io.andygrove.kquery.execution

import io.andygrove.kquery.datasource.RecordBatch
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot

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
                filteredVector
            }
            else -> TODO()
        }
    }
}
