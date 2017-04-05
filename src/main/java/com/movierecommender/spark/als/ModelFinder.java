package com.movierecommender.spark.als;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.Rating;

public class ModelFinder {
    Logger logger = Logger.getLogger(ModelFinder.class);

    private int[] ranks = {4, 8, 12};
    private double minError = 100;
    private int bestRank = -1;
    private int bestIterationNr = -1;
    private int[] numIters = {10, 20};

    public TrainedModel findBestModel(JavaRDD<Rating> ratings) {
        double weights[] = {6, 2, 2};
        JavaRDD<Rating>[] randomRatings = ratings.randomSplit(weights, 0L);
        JavaRDD<Rating> trainingRdd = randomRatings[0];
        JavaRDD<Rating> validationRdd = randomRatings[1];
        JavaRDD<Rating> testRdd = randomRatings[2];
        TrainConfig trainConfig = findBestTrainingParameters(trainingRdd, validationRdd);

        TrainedModel model = ModelFactory.create(trainingRdd, testRdd, trainConfig.getRankNr(),
                trainConfig.getIterationsNr());
        logger.info("best model have RMSE = " + model.getError());

        return model;
    }

    private TrainConfig findBestTrainingParameters(JavaRDD<Rating> trainingRdd, JavaRDD<Rating> validationRdd) {
        for (int numIter : numIters) {
            for (int rank : ranks) {
                TrainedModel model = ModelFactory.create(trainingRdd, validationRdd, rank, numIter);
                if (model.getError() < minError) {
                    minError = model.getError();
                    bestRank = rank;
                    bestIterationNr  = numIter;
                }
            }
        }
        TrainConfig trainConfig = new TrainConfig(bestIterationNr, bestRank);
        logger.info(String.format("The best model was trained with rank %d and iterations number %d",
                trainConfig.getRankNr(), trainConfig.getIterationsNr()));

        return trainConfig;
    }
}
