package io.andygrove.kquery

import java.sql.SQLException

/**
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 */
class SqlPlanner {

    /**
     * Create logical plan from parsed SQL statement.
     */
    fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

        // get a reference to the data source
        var df = tables[select.tableName] ?: throw SQLException("No table named '${select.tableName}'")

        // apply projection
        df = df.select(select.projection.map { createLogicalExpr(it, df) })

        // wrap in a selection (filter)
        if (select.selection != null) {
            df = df.filter(createLogicalExpr(select.selection, df))
        }

        return df
    }

    private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
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