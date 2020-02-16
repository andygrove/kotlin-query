package kquery;

interface Expr {
    fun toField(input: LogicalPlan): Field
}

class Column(val i: Int): Expr {
    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }
}

class LiteralLong(val n: Long): Expr {
    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), DataType.Long)
    }
}

interface LogicalPlan {
    fun schema(): Schema
}

class Projection(val input: LogicalPlan, val expr: List<Expr>): LogicalPlan {
    override fun schema(): Schema {
        return Schema(expr.map { it.toField(input) })
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

class Schema(val fields: List<Field>)
