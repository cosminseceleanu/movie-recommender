package com.movierecommender.spark;

import com.movierecommender.model.Movie;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

public class Engine {
    public static final String moviesPath = "/opt/spark/data/ml-100k/movies.csv";
    public static final String ratingsPath = "/opt/spark/data/ml-100k/ratings.csv";

    public static void start(JavaSparkContext sparkContext) {
        //load and parse the data
        JavaRDD<String> moviesData = sparkContext.textFile(moviesPath);
        JavaRDD<String> ratingsData = sparkContext.textFile(ratingsPath);

        JavaRDD<Movie> movies = moviesData.map(line -> {
            String[] lineParts = line.split(",");
            int movieId = Integer.parseInt(lineParts[0]);
            return new Movie(movieId, lineParts[1], lineParts[2]);
        });
        JavaRDD<Rating> ratings = ratingsData.map(line -> {
            String[] lineParts = line.split(",");
            int userId = Integer.parseInt(lineParts[0]);
            int movieId = Integer.parseInt(lineParts[1]);
            double rating = Double.parseDouble(lineParts[2]);
            return new Rating(userId, movieId, rating);
        });

        //selecting als parameters
        double weights[] = {6, 2, 2};
        JavaRDD<Rating>[] randomRatings = ratings.randomSplit(weights, 0L);
        JavaRDD<Rating> trainingRdd = randomRatings[0];
        JavaRDD<Rating> validationRdd = randomRatings[1];
        JavaRDD<Rating> testRdd = randomRatings[2];
        System.out.println("raitings -> " + ratings.count());
        System.out.println("training -> " + trainingRdd.count());
        System.out.println("validation -> " + validationRdd.count());
        System.out.println("test -> " + testRdd.count());
        ModelTrainer.train(trainingRdd, testRdd, validationRdd);
    }
}
