package com.movierecommender.main.io;

import com.movierecommender.spark.SparkContextAware;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.Rating;

public class RatingsFileIo implements IoOperation<Rating>, SparkContextAware {
    private static final String ratingsPath = "/opt/spark/data/ml-100k/ratings.csv";
    private JavaSparkContext sparkContext;

    @Override
    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    @Override
    public JavaRDD<Rating> readInput() {
        JavaRDD<String> data = sparkContext.textFile(ratingsPath);

        return data.map(line -> {
            String[] lineParts = line.split(",");
            int userId = Integer.parseInt(lineParts[0]);
            int movieId = Integer.parseInt(lineParts[1]);
            double rating = Double.parseDouble(lineParts[2]);

            return new Rating(userId, movieId, rating);
        });
    }

    @Override
    public void writeOutput(JavaRDD<Rating> javaRDD) {
        javaRDD.saveAsTextFile("");
    }
}
