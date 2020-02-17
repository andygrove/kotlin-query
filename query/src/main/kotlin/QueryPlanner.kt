package kquery

class QueryPlanner {

    //TODO optimization rules

    /**
     * Create a physical plan from a logical plan.
     */
    fun createPhysicalPlan(plan: LogicalPlan, input: PhysicalPlan? = null) : PhysicalPlan {
        return when (plan) {
            is DataSource -> DataSourceExec(plan, listOf())
            is Selection -> SelectionExec(createPhysicalPlan(plan.input), plan)
            is Projection -> ProjectionExec(createPhysicalPlan(plan.input), plan.schema(), plan.expr.map { createPhysicalExpr(it) })
            else -> throw IllegalStateException()
        }
    }

    /**
     * Create a physical expression from a logical expression.
     */
    fun createPhysicalExpr(expr: Expr): PhysicalExpr {
        return when (expr) {
            is Column -> ColumnExpr(expr)
            else -> throw IllegalStateException()
        }

    }
}