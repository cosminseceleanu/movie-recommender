package com.movierecommender.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

public class ModelTrainer {
    private static int[] ranks = {4, 8, 12};
    private static double minError = 0.0;
    private static int bestRank = -1;

    public static void train(JavaRDD<Rating> trainingRdd, JavaRDD<Rating> testRdd, JavaRDD<Rating> validationRdd) {
//        JavaRDD<Tuple2<Object, Object>> validationForPredict = validationRdd.map(rating -> {
//           return new Tuple2<>(rating.user(), rating.product());
//        });
        int seed = 5;
        int iterations = 10;
        int regularizationParameter = 1;

        for (int rank : ranks) {
            MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingRdd), rank, seed, iterations, regularizationParameter);
            Double error = computeRMSE(model, validationRdd);
            System.out.println(String.format("For rank %d the RMSE is %f", rank, error));
            if (error < minError) {
                minError = error;
                bestRank = rank;
            }
        }
        System.out.println("The best model was trained with rank " + bestRank);
    }

    private static Double computeRMSE(MatrixFactorizationModel model, JavaRDD<Rating> data) {
        JavaRDD<Tuple2<Object, Object>> userProducts = data.map(r -> {
            return  new Tuple2<Object, Object>(r.user(), r.product());
        });


        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
            model.predict(userProducts.rdd())
                .toJavaRDD()
                .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())));


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
