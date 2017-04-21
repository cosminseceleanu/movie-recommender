package com.movierecommender.spark;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.inject.Inject;
import com.movierecommender.spark.model.RawRating;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingEngine {
    private Logger logger = Logger.getLogger(StreamingEngine.class);
    private JavaStreamingContext streamingContext;

    @Inject
    public StreamingEngine(JavaStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }

    public void stream(JavaInputDStream<ConsumerRecord<String, RawRating>> stream) {
        logger.info("start spark streaming v2");
        JavaDStream<RawRating> ratingsRdd = stream.map(stringConsumerRecord -> stringConsumerRecord.value());
        ratingsRdd.foreachRDD((ratingJavaRDD, time) -> {
            if (ratingJavaRDD.count() == 0) {
                logger.info("skip saving to cassandra 0 ratings received");
                return;
            }
            logger.info("Start saving data to cassandra");
            CassandraJavaUtil.javaFunctions(ratingJavaRDD)
                    .writerBuilder("dev", "ratings", CassandraJavaUtil.mapToRow(RawRating.class))
                    .saveToCassandra();
            logger.info("Done saving to cassandra");
        });

        startStreaming();
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
