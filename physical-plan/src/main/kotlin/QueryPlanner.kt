package io.andygrove.kquery

import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.LogicalExpr
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.logical.Projection
import io.andygrove.kquery.logical.Selection
import io.andygrove.kquery.logical.LiteralLong
import io.andygrove.kquery.logical.LiteralDouble
import io.andygrove.kquery.logical.LiteralString
import io.andygrove.kquery.logical.Alias
import io.andygrove.kquery.logical.Column
import io.andygrove.kquery.logical.ColumnIndex
import io.andygrove.kquery.logical.Eq
import io.andygrove.kquery.logical.Mult

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
        is LiteralLong -> LiteralLongPExpr(expr.n)
        is LiteralDouble -> LiteralDoublePExpr(expr.n)
        is LiteralString -> LiteralStringPExpr(expr.str)
        is ColumnIndex -> ColumnPExpr(expr.i)
        is Alias -> {
            // note that there is no physical expression for an alias since the alias
            // only affects the name using in the planning phase and not how the aliased
            // expression is executed
            createPhysicalExpr(expr.expr, input)
        }
        is Column -> {
            val i = input.schema().fields.indexOfFirst { it.name == expr.name }
            if (i == -1) {
                throw SQLException("No column named '${expr.name}'")
            }
            ColumnPExpr(i)
        }
        is Eq -> EqExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //TODO other comparison ops
        is Mult -> MultExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        //TODO other math ops
        //TODO boolean ops
        //is And -> AndExpr(createPhysicalExpr(expr.l, input), createPhysicalExpr(expr.r, input))
        else -> throw IllegalStateException(expr.javaClass.toString())
    }
}