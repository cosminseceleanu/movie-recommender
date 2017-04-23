package com.movierecommender.main.di;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.movierecommender.main.io.CassandraIo;
import com.movierecommender.main.kafka.JsonDeserializer;
import com.movierecommender.spark.als.ModelFinder;
import com.movierecommender.main.model.RawRating;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class SparkModule extends AbstractModule {

    private JavaSparkContext sparkContext = null;
    private JavaStreamingContext streamingContext = null;
    private CassandraIo<RawRating> ratingCassandraIo = null;

    @Override
    protected void configure() {
        // Turn off unnecessary logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    @Provides
    ModelFinder provideModelFinder() {
        return new ModelFinder();
    }

    @Provides
    SparkConf provideSparkConf() {
        return new SparkConf()
//                .setMaster("spark://127.0.1.1:7077")
                .setMaster("local[*]")
                .setJars(new String[]{"target/movie-recommender-1.0-SNAPSHOT-jar-with-dependencies.jar"})
                .setAppName("Movie Recommendation")
                .set("spark.executor.memory", "4g")
                .set("spark.cassandra.connection.host", "172.18.0.4");
    }

    @Provides
    JavaSparkContext providesJavaSparkContext(SparkConf sparkConf) {
        if (sparkContext != null) {
            return sparkContext;
        }
        sparkContext = new JavaSparkContext(sparkConf);

        return sparkContext;
    }

    @Provides
    JavaStreamingContext provideStreamingContext(JavaSparkContext sparkContext) {
        if (streamingContext != null) {
            return streamingContext;
        }
        streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(5));

        return streamingContext;
    }

    @Provides
    JavaInputDStream<ConsumerRecord<String, RawRating>> providesKafkaInputStream(JavaStreamingContext streamingContext) {
        Map<String, Object> kafkaParams = new HashedMap();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", JsonDeserializer.class);
        kafkaParams.put("serializedClass", RawRating.class);
        kafkaParams.put("group.id", "rating_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Collection<String> topics = Arrays.asList("topicA", "topicB");

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, RawRating>Subscribe(topics, kafkaParams)
        );
    }

    @Provides
    CassandraIo<RawRating> providesCassandraRatingIO(JavaSparkContext sparkContext) {
        if (ratingCassandraIo != null) {
            return ratingCassandraIo;
        }
        ratingCassandraIo = new CassandraIo<>(RawRating.class, "dev", "ratings");
        ratingCassandraIo.setSparkContext(sparkContext);


        return ratingCassandraIo;
    }
}
