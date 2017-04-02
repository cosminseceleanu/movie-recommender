package com.movierecommender;

import com.movierecommender.spark.Engine;
import com.movierecommender.spark.train.ModelFinder;
import com.movierecommender.spark.train.TrainConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RecommendationEngine {
    public static void main(String[] args) {
        // Turn off unnecessary logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setMaster("spark://127.0.1.1:7077")
                .setJars(new String[]{"build/libs/movie-recommender-1.0.jar"})
                .setAppName("Movie Recommendation");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        ModelFinder modelFinder = new ModelFinder();
        Engine engine = new Engine(sparkContext, modelFinder);
        TrainConfig trainConfig = new TrainConfig(10, 4);
        engine.start(trainConfig);
        engine.test();
    }
}
