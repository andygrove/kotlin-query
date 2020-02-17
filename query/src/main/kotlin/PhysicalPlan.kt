package kquery

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

interface PhysicalPlan {
    fun execute(): Iterable<RecordBatch>
}

/**
 * Scan a data source with optional push-down projection.
 */
class DataSourceExec(val ds: DataSource, val projection: List<Int>) : PhysicalPlan {
    override fun execute(): Iterable<RecordBatch> {
        return ds.scan(projection);
    }
}

class ProjectionExec(val input: PhysicalPlan, val projection: Projection) : PhysicalPlan {

    override fun execute(): Iterable<RecordBatch> {
        return input.execute().map { batch ->
            val fieldVectors = projection.expr.map { it.evaluate(batch) }
            RecordBatch(projection.schema(), VectorSchemaRoot(fieldVectors))
        }
    }
}

