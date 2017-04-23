package com.movierecommender.main.io;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.movierecommender.spark.SparkContextAware;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CassandraIo<T> implements IoOperation<T>, SparkContextAware {

    private String db;
    private String table;
    private final Class<T> tClass;
    private JavaSparkContext sparkContext = null;

    public CassandraIo(Class<T> tClass, String db, String table) {
        this.db = db;
        this.table = table;
        this.tClass = tClass;
    }

    @Override
    public JavaRDD<T> readInput() {
        if (sparkContext == null) {
            throw new RuntimeException("to read from cassandra spark context must be set");
        }

        return CassandraJavaUtil.javaFunctions(sparkContext)
                .cassandraTable(db, table, CassandraJavaUtil.mapRowTo(tClass));
    }

    @Override
    public void writeOutput(JavaRDD<T> javaRDD) {
        CassandraJavaUtil.javaFunctions(javaRDD)
                .writerBuilder(db, table, CassandraJavaUtil.mapToRow(tClass))
                .saveToCassandra();
    }

    @Override
    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }
}
