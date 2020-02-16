package kquery;

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    @Ignore
    fun test() {

//        val df = DefaultDataFrame()
//
//        val df2 = df.parquet("/foo/bar")
//            .filter(Eq(Column("a"), LiteralInt(123)))
//            .select(listOf(Column("a"), Column("b"), Column("c")))
//
//        df2.collect().forEach {
//            println("Received batch")
//        }
    }
}