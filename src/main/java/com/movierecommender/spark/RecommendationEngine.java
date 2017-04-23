package com.movierecommender.spark;

import com.google.inject.Inject;
import com.movierecommender.main.io.IoOperation;
import com.movierecommender.main.model.RawRating;
import com.movierecommender.main.model.UserRecommendations;
import com.movierecommender.spark.als.ModelFactory;
import com.movierecommender.spark.als.TrainConfig;
import com.movierecommender.spark.als.TrainedModel;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

public class RecommendationEngine {
    private static Logger logger = Logger.getLogger(RecommendationEngine.class);

    private JavaSparkContext sparkContext;

    @Inject
    public RecommendationEngine(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public TrainedModel train(TrainConfig trainConfig, IoOperation<RawRating> ioOperation) {
        logger.info("loadAndParseRatings ratings data");
        JavaRDD<Rating> ratings = ioOperation.readInput()
                .map(rawRating -> rawRating.toSparkRating());
        return createAlsModel(ratings, trainConfig);
    }

    private TrainedModel createAlsModel(JavaRDD<Rating> ratings, TrainConfig trainConfig) {
        double[] weights = {8, 2};
        JavaRDD<Rating>[] randomRatings = ratings.randomSplit(weights, 0L);

        return ModelFactory.create(randomRatings[0],
                randomRatings[1],
                trainConfig.getRankNr(),
                trainConfig.getIterationsNr()
        );
    }

    public void saveUserRecommendations(TrainedModel model, IoOperation<UserRecommendations> ioOperation) {
        logger.info("start saving user recommendations");
        JavaRDD<Tuple2<Object, Rating[]>> recommendations = model.getMatrixModel()
                .recommendProductsForUsers(20)
                .toJavaRDD();

        logger.info("recommendations count " + recommendations.count());

        JavaRDD<UserRecommendations> userRecommendationsRDD = recommendations.map(tuple -> {
            Set<Integer> products = new HashSet<>();
            for (Rating rating : tuple._2) {
                products.add(rating.product());
            }

            return new UserRecommendations((int) tuple._1(), products);
        });
        ioOperation.writeOutput(userRecommendationsRDD);
    }
}
