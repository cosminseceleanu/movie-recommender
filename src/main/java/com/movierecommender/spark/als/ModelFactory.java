package com.movierecommender.spark.als;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

public class ModelFactory {
    private static Logger logger = Logger.getLogger(ModelFactory.class);

    public static TrainedModel create(JavaRDD<Rating> trainingRdd, JavaRDD<Rating> testRdd, int rank, int iterationsNr) {
        logger.info(String.format("Train with parameters -> iterations: %d, rank :%d", iterationsNr, rank));
        JavaRDD<Tuple2<Object, Object>> testForPredict = testRdd.map(rating ->
            new Tuple2<>(rating.user(), rating.product())
        );

        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainingRdd), rank, iterationsNr, 0.1);
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(testForPredict.rdd())
                        .toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );

        Double error = computeRMSE(predictions, testRdd);
        logger.info(String.format("For rank %d and iterations %d the RMSE is %f", rank, iterationsNr, error));

        return new TrainedModel(error, model);
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
