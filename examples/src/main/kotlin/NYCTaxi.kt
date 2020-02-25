import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.execution.ExecutionContext
import io.andygrove.kquery.logical.col


fun main() {

    val ctx = ExecutionContext()

    // wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv

    val df = ctx.csv("/home/andy/data/yellow_tripdata_2019-01.csv", 1024)
           // .aggregate(listOf(col("")), listOf())

    val results = ctx.execute(df)

    results.forEach {
        println(it.schema)
        println(it.toCSV())
    }

}