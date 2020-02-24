package io.andygrove.kquery.logical

import org.apache.arrow.vector.types.pojo.Schema

/**
 * Logical plan representing an aggregate query against an input.
 */
class Aggregate(val input: LogicalPlan, val groupExpr: List<LogicalExpr>, val aggregateExpr: List<AggregateExpr>) : LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return super.toString()
    }
}