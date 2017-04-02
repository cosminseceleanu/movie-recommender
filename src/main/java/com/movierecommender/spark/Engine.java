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
        initialBuild(ratings);


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

    private static void initialBuild(JavaRDD<Rating> ratings) {
        // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(r -> new Tuple2<>(r.user(), r.product()));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts))
                        .toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                ratings.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();

        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            return err * err;
        }).mean();
        System.out.println("Mean Squared Error = " + MSE);
    }
}
