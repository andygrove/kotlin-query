package io.andygrove.kquery

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema

import java.sql.SQLException

/**
 * Logical Expression for use in logical query plans. The logical expression provides information needed
 * during the planning phase such as the name and data type of the expression.
 */
interface LogicalExpr {

    /**
     * Return meta-data about the value that will be produced by this expression when evaluated against
     * a particular input.
     */
    fun toField(input: LogicalPlan): Field
}

infix fun LogicalExpr.eq(rhs: LogicalExpr): Eq {
    return Eq(this, rhs)
}

/**
 * Logical expression representing a reference to a column by name.
 */
class Column(val name: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields.find { it.name == name } ?: throw SQLException("No column named '$name'")
    }

    override fun toString(): String {
        return "#$name"
    }

}

/** Convenience method to create a Column reference */
fun col(name: String) = Column(name)

/**
 * Logical expression representing a reference to a column by index.
 */
class ColumnIndex(val i: Int): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }

    override fun toString(): String {
        return "#$i"
    }

}

/**
 * Logical expression representing a literal string value.
 */
class LiteralString(val str: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(str, ArrowType.Utf8())
    }

    override fun toString(): String {
        return "'$str'"
    }

}

/** Convenience method to create a LiteralString */
fun lit(value: String) = LiteralString(value)

/**
 * Logical expression representing a literal long value.
 */
class LiteralLong(val n: Long): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(n.toString(), ArrowType.Int(32, false))
    }

    override fun toString(): String {
        return n.toString()
    }

}

/** Convenience method to create a LiteralLong */
fun lit(value: Long) = LiteralLong(value)

/**
 * Logical expression representing an equality comparison.
 */
class Eq(val l: LogicalExpr, val r: LogicalExpr): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullablePrimitive("eq", ArrowType.Bool())
    }

    override fun toString(): String {
        return "$l = $r"
    }

}

/**
 * Base interface for all aggregate expressions.
 */
interface AggregateExpr {

    /**
     * Return meta-data about the value that will be produced by this expression when evaluated against
     * a particular input.
     */
    fun toField(input: LogicalPlan): Field
}

/**
 * Logical expression representing the SUM aggregate expression.
 */
class Sum(val e: LogicalExpr) : AggregateExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field.nullable("sum", e.toField(input).type)
    }

    override fun toString(): String {
        return "SUM($e)"
    }
}

/**
 * A logical plan represents a data transformation or action that returns a relation (a set of tuples).
 */
interface LogicalPlan {

    /**
     * Returns the schema of the data that will be produced by this logical plan.
     */
    fun schema(): Schema

    /**
     * Returns the children (inputs) of this logical plan. This method is used to enable use of the
     * visitor pattern to walk a query tree.
     */
    fun children(): List<LogicalPlan>
}

/**
 * Logical plan representing a projection (evaluating a list of expressions) against an input
 */
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

/**
 * Logical plan representing a selection (a.k.a. filter) against an input
 */
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

/**
 * Logical plan representing an aggregate query against an input.
 */
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
