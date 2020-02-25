import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.execution.ExecutionContext
import io.andygrove.kquery.logical.Max
import io.andygrove.kquery.logical.cast
import io.andygrove.kquery.logical.col
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import kotlin.system.measureTimeMillis


fun main() {

    val ctx = ExecutionContext()

    // wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv

    /*
    VendorID: Utf8,
    tpep_pickup_datetime: Utf8,
    tpep_dropoff_datetime: Utf8,
    passenger_count: Utf8,
    trip_distance: Utf8,
    RatecodeID: Utf8,
    store_and_fwd_flag: Utf8,
    PULocationID: Utf8,
    DOLocationID: Utf8,
    payment_type: Utf8,
    fare_amount: Utf8,
    extra: Utf8,
    mta_tax: Utf8,
    tip_amount: Utf8,
    tolls_amount: Utf8,
    improvement_surcharge: Utf8,
    total_amount: Utf8,
    congestion_surcharge: Utf8
    */

    val time = measureTimeMillis {
        val df = ctx.csv("/home/andy/data/yellow_tripdata_2019-01.csv", 1*1024)
                .aggregate(
                        listOf(col("passenger_count")),
                        listOf(Max(cast(col("fare_amount"), ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))))

        val results = ctx.execute(df)

        results.forEach {
            println(it.schema)
            println(it.toCSV())
        }
    }

    println("Query took $time ms")

}