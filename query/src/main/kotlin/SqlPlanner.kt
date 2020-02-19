package kquery

import java.sql.SQLException

class SqlPlanner {

    /**
     * Create logical plan from parsed SQL statement.
     */
    fun createLogicalPlan(select: SqlSelect, tables: Map<String, DataFrame>) : LogicalPlan {

        val df = tables[select.tableName]
        if (df == null) {
            throw SQLException("No table named '${select.tableName}'")
        }

        // TODO selection
        // TODO aggregate

        val input = df.logicalPlan()

        return Projection(input, select.projection.map { createLogicalExpr(it, input) })
    }

    private fun createLogicalExpr(expr: SqlExpr, input: LogicalPlan) : LogicalExpr {
        return when (expr) {
            is Identifier -> Column(input.schema().fields.indexOfFirst { it.name == expr.id })
            else -> TODO()
        }
    }

}