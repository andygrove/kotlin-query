package io.andygrove.kquery

interface DataFrame {

    /** Apply a projection */
    fun select(expr: List<LogicalExpr>): DataFrame

    /** Apply a filter */
    fun filter(expr: LogicalExpr): DataFrame

    /** Execute the query and collect the results */
    fun collect(): Iterator<RecordBatch>

    /** Get the logical plan */
    fun logicalPlan() : LogicalPlan

}

/** Execution context */
class ExecutionContext {

    /** Tables registered with this context */
    private val tables = mutableMapOf<String, DataFrame>()

    /** Create a DataFrame for the given SQL Select */
    fun sql(sql: String): DataFrame {
        val tokens = SqlTokenizer(sql).tokenize()
        val ast = SqlParser(tokens).parse() as SqlSelect
        val df = SqlPlanner().createDataFrame(ast, tables)
        return DataFrameImpl(this, df.logicalPlan())
    }

    /** Get a DataFrame representing the specified CSV file */
    fun csv(filename: String, batchSize: Int = 1000): DataFrame {
        return DataFrameImpl(this, Scan(filename, CsvDataSource(filename, batchSize), listOf()))
    }

    /** Register a DataFrame with the context */
    fun register(tablename: String, df: DataFrame) {
        tables[tablename] = df
    }

    fun execute(plan: LogicalPlan) : Iterable<RecordBatch> {
        val physicalPlan = QueryPlanner().createPhysicalPlan(plan)
        return physicalPlan.execute()
    }

}

class DataFrameImpl(private val ctx: ExecutionContext, private val plan: LogicalPlan) : DataFrame {

    override fun select(expr: List<LogicalExpr>): DataFrame {
        return DataFrameImpl(ctx, Projection(plan, expr))
    }

    override fun filter(expr: LogicalExpr): DataFrame {
        return DataFrameImpl(ctx, Selection(plan, expr))
    }

    override fun collect(): Iterator<RecordBatch> {
        return ctx.execute(plan).iterator()
    }

    override fun logicalPlan(): LogicalPlan {
        return plan
    }


}

