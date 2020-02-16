package kquery;

sealed class Expr {
}

class Column(val name: String): Expr()
class ColumnIndex(val i: Int): Expr()
class LiteralInt(val n: Int): Expr()
class Add(val expr: Expr) : Expr()
class Subtract(val expr: Expr) : Expr()
class Multiply(val expr: Expr) : Expr()
class Divide(val expr: Expr) : Expr()
class Eq(val l: Expr, val r: Expr) : Expr()
class And(val l: Expr, val r: Expr) : Expr()
class Or(val l: Expr, val r: Expr) : Expr()

interface LogicalPlan {
    fun schema(): Schema
}

class Projection(val input: LogicalPlan, val expr: List<Expr>): LogicalPlan {
    override fun schema(): Schema {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class Selection(val input: LogicalPlan, val expr: Expr): LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }
}

class Csv(val filename: String, val schema: Schema): LogicalPlan {
    override fun schema(): Schema {
        return schema
    }
}

enum class DataType {
    String,
    Long,
    Double
}

data class Field(val name: String, val dataType: DataType)

class Schema(val fields: Field)
