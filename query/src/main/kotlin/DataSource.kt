package kquery

import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.vector.types.pojo.Schema

interface DataSource {
    fun schema(): Schema
    fun scan(columns: List<Int>): Iterator<RecordBatch>
}

class CsvDataSource(filename: String) : DataSource {

    override fun schema(): Schema {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun scan(columns: List<Int>): Iterator<RecordBatch> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}