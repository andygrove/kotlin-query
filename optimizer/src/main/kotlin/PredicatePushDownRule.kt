package io.andygrove.kquery.optimizer

import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Projection
import io.andygrove.kquery.logical.Scan

class PredicatePushDownRule : OptimizerRule {

    override fun optimize(plan: LogicalPlan): LogicalPlan {
        return optimize(plan, mutableSetOf())
    }

    private fun optimize(plan: LogicalPlan,
                         columnNames: MutableSet<String>): LogicalPlan {

        return when (plan) {
            is Projection -> {
                extractColumns(plan.expr, columnNames)
                Projection(optimize(plan.input, columnNames), plan.expr)
            }
            is Scan -> {
                Scan(plan.name, plan.dataSource, columnNames.toList().sorted())
            }
            else -> plan
        }
    }

}