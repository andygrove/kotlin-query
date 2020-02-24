package io.andygrove.kquery.physical

import io.andygrove.kquery.datasource.RecordBatch
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

/**
 * Execute a projection.
 */
class ProjectionExec(val input: PhysicalPlan, val schema: Schema, val expr: List<PhysicalExpr>) : PhysicalPlan {

    override fun execute(): Iterable<RecordBatch> {
        return input.execute().map { batch ->

            println("projection input:\n${batch.toCSV()}")

            val fieldVectors = expr.map { it.evaluate(batch) }
            val projectedBatch = RecordBatch(schema, fieldVectors)

            println("projection output:\n${projectedBatch.toCSV()}")

            projectedBatch
        }
    }
}