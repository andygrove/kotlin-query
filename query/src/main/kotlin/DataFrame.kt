package kquery;

interface DataFrame {

    /** Apply a projection */
    fun select(expr: List<Expr>): DataFrame

    /** Apply a filter */
    fun filter(expr: Expr): DataFrame

    /** Execute the query and collect the results */
    fun collect(): Iterator<RecordBatch>

}

class ExecutionContext {

    fun csv(filename: String, batchSize: Int = 1000): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, batchSize), listOf()))
    }

}

class DataFrameImpl(val plan: LogicalPlan) : DataFrame {

    override fun select(expr: List<Expr>): DataFrame {
        return DataFrameImpl(Projection(plan, expr))
    }

    override fun filter(expr: Expr): DataFrame {
        return DataFrameImpl(Selection(plan, expr))
    }

    override fun collect(): Iterator<RecordBatch> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

