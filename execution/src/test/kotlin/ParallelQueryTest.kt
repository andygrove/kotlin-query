package io.andygrove.kquery.execution

//import kotlinx.coroutines.*
import io.andygrove.kquery.physical.*
import io.andygrove.kquery.datasource.RecordBatch
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParallelQueryTest {

    @Test
    @Ignore
    fun `parallel query example`() {

        val list = mutableListOf<List<RecordBatch>>()

        0.rangeTo(10).forEach {
//            list.add(GlobalScope.launch {
//                query("partition-$it.csv")
//            })
        }



    }

    private fun query(filename: String) : List<RecordBatch> {
        TODO()
    }

}