package com.movierecommender.main.di;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.movierecommender.spark.als.ModelFinder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkModule extends AbstractModule {
    @Override
    protected void configure() {
        // Turn off unnecessary logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    @Provides
    ModelFinder provideModelFinder() {
        return new ModelFinder();
    }

    @Provides
    SparkConf provideSparkConf() {
        return new SparkConf()
                .setMaster("spark://127.0.1.1:7077")
                .setJars(new String[]{"build/libs/movie-recommender-1.0.jar"})
                .setAppName("Movie Recommendation");
    }

    @Provides
    JavaSparkContext provideSparkContext(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }
}
