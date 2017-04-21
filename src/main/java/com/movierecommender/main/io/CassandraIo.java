package com.movierecommender.main.io;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;

public class CassandraIo<T> implements IoOperation<T> {

    private String db;
    private String table;
    private final Class<T> tClass;

    public CassandraIo(Class<T> tClass, String db, String table) {
        this.db = db;
        this.table = table;
        this.tClass = tClass;
    }

    @Override
    public JavaRDD<T> readInput() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void writeOutput(JavaRDD<T> javaRDD) {
        CassandraJavaUtil.javaFunctions(javaRDD)
                .writerBuilder(db, table, CassandraJavaUtil.mapToRow(tClass))
                .saveToCassandra();
    }
}
