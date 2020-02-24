package io.andygrove.kquery.logical

interface DataFrame {

    /** Apply a projection */
    fun select(expr: List<LogicalExpr>): DataFrame

    /** Apply a filter */
    fun filter(expr: LogicalExpr): DataFrame

    /** Execute the query and collect the results */
    //fun collect(): Iterator<RecordBatch>

    /** Get the logical plan */
    fun logicalPlan() : LogicalPlan

}

class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {

    override fun select(expr: List<LogicalExpr>): DataFrame {
        return DataFrameImpl(Projection(plan, expr))
    }

    override fun filter(expr: LogicalExpr): DataFrame {
        return DataFrameImpl(Selection(plan, expr))
    }

//    override fun collect(): Iterator<RecordBatch> {
//        return ctx.execute(plan).iterator()
//    }

    override fun logicalPlan(): LogicalPlan {
        return plan
    }


}

