package kquery;

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema

interface LogicalExpr {
    fun toField(input: LogicalPlan): Field
}

class Column(val i: Int): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }

    override fun toString(): String {
        return "#$i"
    }

}

class LiteralString(val str: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(str, ArrowType.Utf8())
    }

    override fun toString(): String {
        return str
    }

}

class LiteralLong(val n: Long): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(n.toString(), ArrowType.Int(32, false))
    }

    override fun toString(): String {
        return n.toString()
    }

}

class Eq(val l: LogicalExpr, val r: LogicalExpr): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullablePrimitive("eq", ArrowType.Bool())
    }

    override fun toString(): String {
        return "$l = $r"
    }

}

interface AggregateExpr {
    fun toField(input: LogicalPlan): Field
}

class Sum(val e: LogicalExpr) : AggregateExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field.nullable("sum", e.toField(input).type)
    }

    override fun toString(): String {
        return "SUM($e)"
    }
}

interface LogicalPlan {
    fun schema(): Schema
    fun children(): List<LogicalPlan>
}

/** Apply a projection (evaluate a list of expressions) to an input */
class Projection(val input: LogicalPlan, val expr: List<LogicalExpr>): LogicalPlan {
    override fun schema(): Schema {
        return Schema(expr.map { it.toField(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Projection: ${ expr.map { it.toString() }.joinToString(", ") }"
    }
}

/** Apply a selection (a.k.a. filter) to an input */
class Selection(val input: LogicalPlan, val expr: LogicalExpr): LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Selection: $expr"
    }
}

class Aggregate(val input: LogicalPlan, val groupExpr: List<LogicalExpr>, val aggregateExpr: List<AggregateExpr>) : LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return super.toString()
    }
}

/** Represents a scan of a data source */
class Scan(val name: String, val dataSource: DataSource, val projection: List<Int>): LogicalPlan {

    override fun schema(): Schema {
        return dataSource.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return if (projection.isEmpty()) {
            "Scan: $name; projection=None"
        } else {
            "Scan: $name; projection=${ projection.map { "#$it" }.joinToString { "," } }"
        }
    }
}


/** Format a logical plan in human-readable form */
fun format(plan: LogicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent+1)) }
    return b.toString()
}
