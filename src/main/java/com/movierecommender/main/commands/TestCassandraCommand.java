package com.movierecommender.main.commands;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.google.inject.Inject;
import org.apache.spark.api.java.JavaSparkContext;

public class TestCassandraCommand implements Command {
    private JavaSparkContext sparkContext;

    @Inject
    public TestCassandraCommand(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @Override
    public void execute() {
        CassandraTableScanJavaRDD tabel = CassandraJavaUtil.javaFunctions(sparkContext)
                .cassandraTable("dev", "spark_stream_count");

        System.out.println("count: " +  tabel.count());
        System.out.println("firt row " + tabel.first());
    }

    @Override
    public String getName() {
        return "cassandra-test";
    }
}
