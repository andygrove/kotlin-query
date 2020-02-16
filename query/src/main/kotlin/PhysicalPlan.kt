package kquery

import org.apache.arrow.flatbuf.RecordBatch
import org.apache.arrow.vector.types.pojo.Schema


//sealed class PhysicalPlan: DataSource

//    class Projection(val expr: List<Expr>): PhysicalPlan() {
//        override fun iterator(): Iterator<RecordBatch> {
//            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//        }
//    }
//
//    class Selection(val expr: Expr): PhysicalPlan() {
//        override fun iterator(): Iterator<RecordBatch> {
//            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//        }
//    }
//
//    class Csv(val filename: String): PhysicalPlan() {
//        override fun iterator(): Iterator<RecordBatch> {
//            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//        }
//    }
