package kquery

class SqlPlanner {


    /**
     * Create logical plan from parsed SQL statement.
     */
    fun createLogicalPlan(select: SqlSelect) : LogicalPlan {

        val ctx = ExecutionContext()

        var df = ctx.csv(select.tableName + ".csv")

        // TODO projection
        // TODO selection
        // TODO aggregate

        return df.logicalPlan()
    }

//    fun toLogicalExpr(expr: SqlExpr) : Expr {
//        when (expr) {
//            is Identifier
//        }
//
//    }

}