package io.andygrove.kquery

import java.sql.SQLException

/**
 * The query planner creates a physical query plan from a logical query plan.
 */
class QueryPlanner {

    //TODO optimization rules

    /**
     * Create a physical plan from a logical plan.
     */
    fun createPhysicalPlan(plan: LogicalPlan) : PhysicalPlan {
        return when (plan) {
            is Scan -> ScanExec(plan.dataSource, plan.projection)
            is Selection -> SelectionExec(createPhysicalPlan(plan.input), createPhysicalExpr(plan.expr, plan.input))
            is Projection -> ProjectionExec(createPhysicalPlan(plan.input), plan.schema(), plan.expr.map { createPhysicalExpr(it, plan.input) })
            else -> throw IllegalStateException(plan.javaClass.toString())
        }
    }

    /**
     * Create a physical expression from a logical expression.
     */
    fun createPhysicalExpr(expr: LogicalExpr, input: LogicalPlan): PhysicalExpr = when (expr) {
        is LiteralLong -> TODO()
        is LiteralString -> TODO()
        is ColumnIndex -> ColumnExpr(expr.i)
        is Column -> {
            val i = input.schema().fields.indexOfFirst { it.name == expr.name }
            if (i == -1) {
                throw SQLException("No column named '${expr.name}'")
            }
            ColumnExpr(i)
        }
        is Eq -> EqExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        else -> throw IllegalStateException(expr.javaClass.toString())
    }
}