package kquery;

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
        val tokens = Tokenizer(sql).tokenize()
        val ast = SqlParser(tokens).parse() as SqlSelect
        val plan1 = SqlPlanner().createLogicalPlan(ast, tables)
        return DataFrameImpl(plan1)
    }

    /** Get a DataFrame representing the specified CSV file */
    fun csv(filename: String, batchSize: Int = 1000): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, batchSize), listOf()))
    }

    /** Register a DataFrame with the context */
    fun register(tablename: String, df: DataFrame) {
        tables[tablename] = df
    }

}

class DataFrameImpl(val plan: LogicalPlan) : DataFrame {

    override fun select(expr: List<LogicalExpr>): DataFrame {
        return DataFrameImpl(Projection(plan, expr))
    }

    override fun filter(expr: LogicalExpr): DataFrame {
        return DataFrameImpl(Selection(plan, expr))
    }

    override fun collect(): Iterator<RecordBatch> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun logicalPlan(): LogicalPlan {
        return plan
    }


}

