package io.andygrove.kquery.optimizer

import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Projection
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.logical.Selection

class ProjectionPushDownRule : OptimizerRule {

    override fun optimize(plan: LogicalPlan): LogicalPlan {
        return pushDown(plan, mutableSetOf())
    }

    private fun pushDown(plan: LogicalPlan,
                         columnNames: MutableSet<String>): LogicalPlan {

        println("optimize() $plan, columnNames=$columnNames")

        return when (plan) {
            is Projection -> {
                extractColumns(plan.expr, columnNames)
                Projection(pushDown(plan.input, columnNames), plan.expr)
            }
            is Selection -> {
                extractColumns(plan.expr, columnNames)
                Selection(pushDown(plan.input, columnNames), plan.expr)
            }
            is Scan -> Scan(plan.name, plan.dataSource, columnNames.toList().sorted())
            else -> TODO()
        }
    }

}