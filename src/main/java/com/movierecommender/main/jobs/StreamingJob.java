package com.movierecommender.main.jobs;


import com.google.inject.Inject;
import com.movierecommender.spark.Engine;
import com.movierecommender.spark.StreamingEngine;
import com.movierecommender.spark.als.TrainConfig;
import com.movierecommender.spark.model.RawRating;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;

public class StreamingJob implements Job {
    private Engine engine;
    private StreamingEngine streamingEngine;
    private JavaInputDStream<ConsumerRecord<String, RawRating>> inputDStream;

    @Inject
    public StreamingJob(Engine engine, StreamingEngine streamingEngine,
                        JavaInputDStream<ConsumerRecord<String, RawRating>> inputDStream) {
        this.engine = engine;
        this.streamingEngine = streamingEngine;
        this.inputDStream = inputDStream;
    }

    @Override
    public void execute() {
        TrainConfig trainConfig = new TrainConfig(10, 4);
        engine.train(trainConfig);
        streamingEngine.stream(inputDStream);
    }

    @Override
    public String getName() {
        return Job.DEFAULT_COMMAND;
    }
}
