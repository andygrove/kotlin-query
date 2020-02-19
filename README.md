# kotlin-query

`kotlin-query` is an in-memory SQL query engine based on Apache Arrow. It supports both a DataFrame API and SQL.

SQL Example:

```kotlin
// Create a context
val ctx = ExecutionContext()

// Register a CSV data source with the context 
val csv: DataFrame = ctx.csv(employeeCsv)
ctx.register("employee", csv)

// Execute a SQL query 
val df: DataFrame = ctx.sql("SELECT id FROM employee")
val result = df.collect()
```

DataFrame Example:

```kotlin
// Create a context
val ctx = ExecutionContext()

// Construct a query using the DataFrame API
val df: DataFrame = ctx.csv("employee.csv")
    .filter(col("state") eq "CO")
    .select(listOf(col("id"), col("first_name"), col("last_name")))

// Execute the query
val result = df.collect()

```

# Documentation

See the companion book [How Query Engines Work](https://leanpub.com/how-query-engines-work/) for design documentation.

# Status

This is a work in progress. Check back soon.