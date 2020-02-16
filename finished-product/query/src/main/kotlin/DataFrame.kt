package kquery;

interface DataFrame {

    /** Apply a projection */
    fun select(expr: List<Expr>): DataFrame

    /** Apply a filter */
    fun filter(expr: Expr): DataFrame

    /** Read a parquet data source at the given path */
    fun parquet(filename: String): DataFrame

    /** Execute the query and collect the results */
    fun collect(): Iterator<RecordBatch>

}

interface RecordBatch {
    //TODO
}



class DefaultDataFrame : DataFrame {

    override fun select(expr: List<Expr>): DataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun filter(expr: Expr): DataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun parquet(filename: String): DataFrame {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun collect(): Iterator<RecordBatch> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

