package com.movierecommender.main.jobs;


import com.google.inject.Inject;
import com.movierecommender.main.io.CassandraIo;
import com.movierecommender.main.model.RawRating;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingJob implements Job {
    private static Logger logger = Logger.getLogger(StreamingJob.class);
    private JavaInputDStream<ConsumerRecord<String, RawRating>> inputDStream;
    private JavaStreamingContext streamingContext;
    private CassandraIo<RawRating> ratingsIo;

    @Inject
    public StreamingJob(JavaStreamingContext streamingContext,
                        JavaInputDStream<ConsumerRecord<String, RawRating>> inputDStream,
                        CassandraIo<RawRating> ratingsIo
    ) {
        this.streamingContext = streamingContext;
        this.inputDStream = inputDStream;
        this.ratingsIo = ratingsIo;
    }

    @Override
    public void execute() {
        stream(inputDStream);
    }

    private void stream(JavaInputDStream<ConsumerRecord<String, RawRating>> stream) {
        logger.info("start spark streaming v2.1");
        JavaDStream<RawRating> ratingsRdd = stream.map(stringConsumerRecord -> stringConsumerRecord.value());

        ratingsRdd.foreachRDD((ratingJavaRDD, time) -> {
            if (ratingJavaRDD.count() == 0) {
                logger.info("skip saving 0 ratings received");
                return;
            }
            logger.info("Start saving new ratings");
            ratingsIo.writeOutput(ratingJavaRDD);
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

    @Override
    public String getName() {
        return Job.DEFAULT_COMMAND;
    }
}
