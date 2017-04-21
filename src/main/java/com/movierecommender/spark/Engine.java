package com.movierecommender.spark;

import com.google.inject.Inject;
import com.movierecommender.main.io.RatingsFileIo;
import com.movierecommender.spark.als.ModelFactory;
import com.movierecommender.spark.als.ModelFinder;
import com.movierecommender.spark.als.TrainConfig;
import com.movierecommender.spark.als.TrainedModel;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.Rating;

public class Engine {
    private static Logger logger = Logger.getLogger(Engine.class);

    private JavaSparkContext sparkContext;
    private ModelFinder modelFinder;
    private JavaRDD<Rating> ratings;
    private TrainedModel model;

    @Inject
    public Engine(JavaSparkContext sparkContext, ModelFinder modelFinder) {
        this.sparkContext = sparkContext;
        this.modelFinder = modelFinder;
    }

    public void findBestModel() {
        modelFinder.findBestModel(ratings);
    }

    public void train(TrainConfig trainConfig) {
        load();
        double[] weights = {8, 2};
        JavaRDD<Rating>[] randomRatings = ratings.randomSplit(weights, 0L);

        model = ModelFactory.create(randomRatings[0], randomRatings[1],
                trainConfig.getRankNr(), trainConfig.getIterationsNr());
    }

    public void retrain(JavaRDD<Rating> newRatingsRdd, TrainConfig trainConfig) {
        logger.info("retrain model");
        ratings.union(newRatingsRdd);

        train(trainConfig);
        logger.info("done retrain model");
//        model.getModel().pr
    }

    private void load() {
        RatingsFileIo ratingsIo = new RatingsFileIo();
        ratingsIo.setSparkContext(sparkContext);
        logger.info("load ratings data");
        ratings = ratingsIo.readInput();
    }
}
