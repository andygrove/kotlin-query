package io.andygrove.kquery.physical

import io.andygrove.kquery.datasource.DataSource
import io.andygrove.kquery.datasource.RecordBatch

/**
 * Scan a data source with optional push-down projection.
 */
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {
    override fun execute(): Sequence<RecordBatch> {
        return ds.scan(projection);
    }
}
