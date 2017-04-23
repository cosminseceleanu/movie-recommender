package com.movierecommender.main.io;

import org.apache.spark.api.java.JavaRDD;

public interface IoOperation<T> {
    JavaRDD<T> readInput();
    void writeOutput(JavaRDD<T> javaRDD);
}
