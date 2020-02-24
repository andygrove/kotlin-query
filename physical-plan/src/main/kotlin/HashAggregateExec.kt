package io.andygrove.kquery.physical

import io.andygrove.kquery.datasource.RecordBatch

class HashAggregateExec(val input: PhysicalPlan, val groupExpr: List<PhysicalExpr>) : PhysicalPlan {

    override fun execute(): Iterable<RecordBatch> {

        val map = HashMap<List<Any>, List<Accumulator>>()

        input.execute().iterator().forEach { batch ->

            println(batch.toCSV())

            // evaluate the grouping expressions
            val groupKeys = groupExpr.map { it.evaluate(batch) }

            (0 until batch.rowCount()).forEach { rowIndex ->
                //map.getOrPut()
            }

            //TODO accumulate
        }

        TODO()
        //return RecordBatch()
    }
}

interface Accumulator {
    fun accumulate(value: Any?)
}

class MinAccumulator : Accumulator {
    override fun accumulate(value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class MaxAccumulator : Accumulator {
    override fun accumulate(value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class SumAccumulator : Accumulator {
    override fun accumulate(value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class AvgAccumulator : Accumulator {
    override fun accumulate(value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class CountAccumulator : Accumulator {
    override fun accumulate(value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class CountDistinctAccumulator : Accumulator {
    override fun accumulate(value: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
