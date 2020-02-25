package io.andygrove.kquery.optimizer

import io.andygrove.kquery.logical.*

interface OptimizerRule {
    fun optimize(plan: LogicalPlan) : LogicalPlan
}


fun extractColumns(expr: List<LogicalExpr>, accum: MutableSet<String>) {
    expr.forEach { extractColumns(it, accum) }
}

fun extractColumns(expr: LogicalExpr, accum: MutableSet<String>) {
    when (expr) {
        is Column -> accum.add(expr.name)
        is BinaryExpr -> {
            extractColumns(expr.l, accum)
            extractColumns(expr.r, accum)
        }
        is Alias -> extractColumns(expr.expr, accum)
        else -> {}
    }
}

