package io.andygrove.kquery.physical

import io.andygrove.kquery.datasource.RecordBatch


/**
 * A physical plan represents an executable piece of code that will produce data.
 */
interface PhysicalPlan {

    /**
     * Execute a physical plan and produce a series of record batches.
     */
    fun execute(): Iterable<RecordBatch>
}





