package com.movierecommender.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

public class ModelTrainer {
    private static int[] ranks = {4, 8, 12};
    private static double minError = 100;
    private static int bestRank = -1;
    private static int bestIterationNr = -1;
    private static int[] numIters = {10, 20};

    public static void train(JavaRDD<Rating> trainingRdd, JavaRDD<Rating> testRdd, JavaRDD<Rating> validationRdd) {
        JavaRDD<Tuple2<Object, Object>> validationForPredict = validationRdd.map(rating ->
            new Tuple2<>(rating.user(), rating.product())
        );
        JavaRDD<Tuple2<Object, Object>> testForPredict = testRdd.map(rating ->
            new Tuple2<>(rating.user(), rating.product())
        );

        for (int numIter : numIters) {
            for (int rank : ranks) {
                trainForRank(trainingRdd, validationRdd, validationForPredict, rank, numIter);
            }
        }

        System.out.println(String.format("The best model was trained with rank %d and iterations number %d", bestRank, bestIterationNr));
    }

    private static void trainForRank(JavaRDD<Rating> trainingRdd, JavaRDD<Rating> validationRdd, JavaRDD<Tuple2<Object, Object>> validationForPredict, int rank, int iterations) {
        System.out.println(String.format("Train with parameters -> iterations: %d, rank :%d", iterations, rank));

        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingRdd), rank, iterations, 0.1);
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
            model.predict(validationForPredict.rdd())
                .toJavaRDD()
                .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );

        Double error = computeRMSE(predictions, validationRdd);
        System.out.println(String.format("For rank %d the RMSE is %f", rank, error));
        if (error < minError) {
            minError = error;
            bestRank = rank;
            bestIterationNr  = iterations;
        }
    }

    private static Double computeRMSE(JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions, JavaRDD<Rating> data) {
        JavaRDD<Tuple2<Double, Double>> predictionsAndRatings =
            JavaPairRDD.fromJavaRDD(data.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions)
                .values();

        double mse = predictionsAndRatings.mapToDouble(pair -> {
            Double err = pair._1() - pair._2();
            return err * err;
        }).mean();

        return Math.sqrt(mse);
    }
}
