# kotlin-query

`kotlin-query` is an in-memory SQL query engine based on Apache Arrow. It supports both a DataFrame API and SQL.

## DataFrame Example

```kotlin
// Create a context
val ctx = ExecutionContext()

// Construct a query using the DataFrame API
val df: DataFrame = ctx.csv("src/test/data/employee.csv")
    .filter(col("state") eq lit("CO"))
    .select(listOf(col("id"), col("first_name"), col("last_name")))

// Execute the query
val result = df.collect()
```

This example results in the following logical query plan:

```
Projection: #id, #first_name, #last_name
  Selection: #state = 'CO'
    Scan: src/test/data/employee.csv; projection=None
```

## SQL Example

SQL queries can be executed against any DataFrame that is registered as a table against the context.

```kotlin
// Create a context
val ctx = ExecutionContext()

// Register a CSV data source with the context 
val csv: DataFrame = ctx.csv("src/test/data/employee.csv")
ctx.register("employee", csv)

// Execute a SQL query 
val df: DataFrame = ctx.sql("SELECT id, first_name, last_name FROM employee WHERE state = 'CO'")
val result = df.collect()
```

This example results in the following logical query plan:

```
Selection: #state = 'CO'
  Projection: #id, #first_name, #last_name
    Scan: src/test/data/employee.csv; projection=None
```

# Documentation

See the companion book [How Query Engines Work](https://leanpub.com/how-query-engines-work/) for design documentation. `kotlin-query` is the reference implementation of the design discussed in this book.

# Status

This is a work in progress. Check back soon.

# Contributing

I am accepting contributions to this repository but please bear in mind that this is implementing the design outlined in the corresponding book so any contributions that do not fit into that design are unlikely to be accepted without prior discussion.

If you are looking for an area to contribute to, take a look at the [open issues](https://github.com/andygrove/kotlin-query/issues).