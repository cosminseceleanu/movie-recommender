package com.movierecommender.spark;

import com.google.inject.Inject;
import com.movierecommender.spark.als.ModelFactory;
import com.movierecommender.spark.als.ModelFinder;
import com.movierecommender.spark.als.TrainConfig;
import com.movierecommender.spark.als.TrainedModel;
import com.movierecommender.spark.model.Movie;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.Rating;

public class Engine {
    private static final String moviesPath = "/opt/spark/data/ml-100k/movies.csv";
    private static final String ratingsPath = "/opt/spark/data/ml-100k/ratings.csv";
    private static Logger logger = Logger.getLogger(Engine.class);

    private JavaSparkContext sparkContext;
    private ModelFinder modelFinder;

    private JavaRDD<Movie> movies;
    private JavaRDD<Rating> ratings;
    private TrainedModel model;

    @Inject
    public Engine(JavaSparkContext sparkContext, ModelFinder modelFinder) {
        this.sparkContext = sparkContext;
        this.modelFinder = modelFinder;
    }

    public void start() {
        load();
        model = modelFinder.findBestModel(ratings);
    }

    public void start(TrainConfig trainConfig) {
        load();
        double[] weights = {8, 2};
        JavaRDD<Rating>[] randomRatings = ratings.randomSplit(weights, 0L);
        model = ModelFactory.create(randomRatings[0], randomRatings[1],
                trainConfig.getRankNr(), trainConfig.getIterationsNr());
    }

    private void load() {
        //load and parse the data
        logger.info("load movies data");
        JavaRDD<String> moviesData = sparkContext.textFile(moviesPath);
        logger.info("load ratings data");
        JavaRDD<String>ratingsData = sparkContext.textFile(ratingsPath);

        movies = moviesData.map(line -> {
            String[] lineParts = line.split(",");
            int movieId = Integer.parseInt(lineParts[0]);
            return new Movie(movieId, lineParts[1], lineParts[2]);
        });
        ratings = ratingsData.map(line -> {
            String[] lineParts = line.split(",");
            int userId = Integer.parseInt(lineParts[0]);
            int movieId = Integer.parseInt(lineParts[1]);
            double rating = Double.parseDouble(lineParts[2]);
            return new Rating(userId, movieId, rating);
        });
    }

    public void test() {
        Rating[] recommendProducts = model.getModel().recommendProducts(1, 5);
        for (Rating rating : recommendProducts) {
            System.out.println(rating);
            System.out.println(movies.filter(movie -> rating.product() == movie.getMovieId()).first());
        }
    }
}
