package com.movierecommender.main.commands;

import com.google.inject.Inject;
import com.movierecommender.spark.StreamingEngine;
import com.movierecommender.spark.model.RawRating;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;

public class TestStreamingCommand implements Command {
    private StreamingEngine streamingEngine;
    private JavaInputDStream<ConsumerRecord<String, RawRating>> stream;

    @Inject
    public TestStreamingCommand(StreamingEngine streamingEngine, JavaInputDStream<ConsumerRecord<String, RawRating>> stream) {
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
