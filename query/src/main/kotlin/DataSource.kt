package io.andygrove.kquery

import com.github.doyaaaaaken.kotlincsv.client.CsvReader

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter

import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.RecordReader
import org.apache.parquet.schema.PrimitiveType

import java.io.File
import java.lang.IllegalStateException


interface DataSource {

    /** Return the schema for the underlying data source */
    fun schema(): Schema

    /** Scan the data source, selecting the specified columns */
    fun scan(columns: List<Int>): Iterable<RecordBatch>
}

