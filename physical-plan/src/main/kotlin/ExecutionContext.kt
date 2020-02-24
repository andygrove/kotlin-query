package io.andygrove.kquery

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.datasource.RecordBatch
import io.andygrove.kquery.logical.DataFrame
import io.andygrove.kquery.logical.DataFrameImpl
import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.sql.*

/** Execution context */
class ExecutionContext {

    /** Tables registered with this context */
    private val tables = mutableMapOf<String, DataFrame>()

    /** Create a DataFrame for the given SQL Select */
    fun sql(sql: String): DataFrame {
        val tokens = SqlTokenizer(sql).tokenize()
        val ast = SqlParser(tokens).parse() as SqlSelect
        val df = SqlPlanner().createDataFrame(ast, tables)
        return DataFrameImpl(df.logicalPlan())
    }

    /** Get a DataFrame representing the specified CSV file */
    fun csv(filename: String, batchSize: Int = 1000): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, batchSize), listOf()))
    }

    /** Register a DataFrame with the context */
    fun register(tablename: String, df: DataFrame) {
        tables[tablename] = df
    }

    fun execute(plan: LogicalPlan) : Iterable<RecordBatch> {
        val physicalPlan = QueryPlanner().createPhysicalPlan(plan)
        return physicalPlan.execute()
    }

    fun execute(df: DataFrame) : Iterable<RecordBatch> {
        return execute(df.logicalPlan())
    }
}