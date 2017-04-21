package com.movierecommender.spark;

import org.apache.spark.api.java.JavaSparkContext;

public interface SparkContextAware {
    void setSparkContext(JavaSparkContext sparkContext);
}
