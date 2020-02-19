# kotlin-query

`kotlin-query` is an in-memory SQL query engine based on Apache Arrow. It supports both a DataFrame API and SQL.

```kotlin
// create an execution context
val ctx = ExecutionContext()

// register a CSV data source with the context 
val csv: DataFrame = ctx.csv(employeeCsv)
ctx.register("employee", csv)

// execute a SQL query 
val df: DataFrame = ctx.sql("SELECT id FROM employee")
val batches = df.collect()

batches.forEach {
    println("got batch with schema: ${it.schema}")
}
```

# Documentation

See the companion book [How Query Engines Work](https://leanpub.com/how-query-engines-work/) for design documentation.

# Status

This is a work in progress. Check back soon.