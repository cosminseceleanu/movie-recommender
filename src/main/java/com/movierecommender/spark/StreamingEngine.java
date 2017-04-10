package com.movierecommender.spark;

import com.google.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingEngine {
    Logger logger = Logger.getLogger(StreamingEngine.class);
    private JavaStreamingContext streamingContext;

    @Inject
    public StreamingEngine(JavaStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }

    public void stream(JavaInputDStream<ConsumerRecord<String, String>> stream) {
        logger.info("start spark streaming v2");
        stream.print();
        stream.count().print();
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
