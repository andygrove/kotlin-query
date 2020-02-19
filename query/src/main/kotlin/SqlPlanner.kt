package io.andygrove.kquery

import java.sql.SQLException

/**
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 */
class SqlPlanner {

    /**
     * Create logical plan from parsed SQL statement.
     */
    fun createLogicalPlan(select: SqlSelect, tables: Map<String, DataFrame>) : LogicalPlan {

        val df = tables[select.tableName] ?: throw SQLException("No table named '${select.tableName}'")

        // TODO selection
        // TODO aggregate

        val input = df.logicalPlan()

        val projection = Projection(input, select.projection.map { createLogicalExpr(it, input) })

        if (select.selection != null) {
            return Selection(projection, createLogicalExpr(select.selection, projection))
        } else {
            return projection
        }
    }

    private fun createLogicalExpr(expr: SqlExpr, input: LogicalPlan) : LogicalExpr {
        return when (expr) {
            is Identifier -> ColumnIndex(input.schema().fields.indexOfFirst { it.name == expr.id })
            else -> TODO()
        }
    }

}