package io.andygrove.kquery.execution

import io.andygrove.kquery.datasource.DataSource
import io.andygrove.kquery.datasource.RecordBatch

/**
 * Scan a data source with optional push-down projection.
 */
class ScanExec(val ds: DataSource, val projection: List<Int>) : PhysicalPlan {
    override fun execute(): Iterable<RecordBatch> {
        return ds.scan(projection);
    }
}
