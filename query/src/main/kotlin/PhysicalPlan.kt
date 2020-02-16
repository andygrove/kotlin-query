package kquery

import javax.xml.crypto.Data

data class Row(val values: List<Object?>)

interface DataSource {
    fun iterator(): Iterator<Row>
}

sealed class PhysicalPlan: DataSource {

    class Projection(val expr: List<Expr>): PhysicalPlan() {
        override fun iterator(): Iterator<Row> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }

    class Selection(val expr: Expr): PhysicalPlan() {
        override fun iterator(): Iterator<Row> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }

    class Csv(val filename: String): PhysicalPlan() {
        override fun iterator(): Iterator<Row> {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }
}
