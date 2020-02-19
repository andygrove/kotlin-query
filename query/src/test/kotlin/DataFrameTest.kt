package io.andygrove.kquery

import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataFrameTest {

    @Test
    @Ignore
    fun test() {

        val ctx = ExecutionContext()

        val df = ctx.csv("employee.csv")
            .filter(Eq(Column(3), LiteralString("CO")))
            .select(listOf(Column(0), Column(1), Column(2), Column(3), Column(4), Column(5)))

        val results = df.collect()
    }
}