package kquery

//import kotlinx.coroutines.*

import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParallelQueryTest {

    @Test
    fun parallelQuery() {

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