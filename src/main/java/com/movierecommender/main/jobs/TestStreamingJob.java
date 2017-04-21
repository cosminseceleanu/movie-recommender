package com.movierecommender.main.jobs;

import com.google.inject.Inject;
import com.movierecommender.spark.StreamingEngine;
import com.movierecommender.spark.model.RawRating;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;

public class TestStreamingJob implements Job {
    private StreamingEngine streamingEngine;
    private JavaInputDStream<ConsumerRecord<String, RawRating>> stream;

    @Inject
    public TestStreamingJob(StreamingEngine streamingEngine, JavaInputDStream<ConsumerRecord<String, RawRating>> stream) {
        this.streamingEngine = streamingEngine;
        this.stream = stream;
    }

    @Override
    public void execute() {
        streamingEngine.stream(stream);
    }

    @Override
    public String getName() {
        return "streaming-test";
    }
}
