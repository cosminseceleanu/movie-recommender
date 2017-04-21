package com.movierecommender.spark;

import com.google.inject.Inject;
import com.movierecommender.main.io.CassandraIo;
import com.movierecommender.spark.model.RawRating;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingEngine {
    private Logger logger = Logger.getLogger(StreamingEngine.class);
    private JavaStreamingContext streamingContext;
    private Engine engine;

    @Inject
    public StreamingEngine(JavaStreamingContext streamingContext, Engine engine) {
        this.streamingContext = streamingContext;
        this.engine = engine;
    }

    public void stream(JavaInputDStream<ConsumerRecord<String, RawRating>> stream) {
        logger.info("start spark streaming v2");
        JavaDStream<RawRating> ratingsRdd = stream.map(stringConsumerRecord -> stringConsumerRecord.value());

        ratingsRdd.foreachRDD((ratingJavaRDD, time) -> {
            if (ratingJavaRDD.count() == 0) {
                logger.info("skip saving to cassandra 0 ratings received");
                return;
            }
            saveToCassandra(ratingJavaRDD);
        });

        startStreaming();
    }

    private void saveToCassandra(JavaRDD<RawRating> ratingJavaRDD) {
        logger.info("Start saving data to cassandra");
        CassandraIo<RawRating> cassandraIo = new CassandraIo<>(RawRating.class, "dev", "ratings");
        cassandraIo.writeOutput(ratingJavaRDD);
        logger.info("Done saving to cassandra");
    }

    private void startStreaming() {
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("fail streaming", e);
        }
    }
}
