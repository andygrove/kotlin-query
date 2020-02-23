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
            is SqlIdentifier -> Column(expr.id)
            is SqlString -> LiteralString(expr.value)
            is SqlLong -> LiteralLong(expr.value)
            is SqlDouble -> LiteralDouble(expr.value)
            is SqlBinaryExpr -> when(expr.op) {
                // comparison operators
                "=" -> Eq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "!=" -> Neq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                ">" -> Gt(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                ">=" -> GtEq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "<" -> Lt(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "<=" -> LtEq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                // boolean operators
                "AND" -> And(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "OR" -> Or(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                // math operators
                "*" -> Mult(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                //TODO add other math operations
                else -> TODO("Binary operator ${expr.op}")
            }
            is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
            else -> TODO(expr.javaClass.toString())
        }
    }

}