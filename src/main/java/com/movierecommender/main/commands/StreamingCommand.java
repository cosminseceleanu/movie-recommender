package com.movierecommender.main.commands;


import com.google.inject.Inject;
import com.movierecommender.spark.Engine;
import com.movierecommender.spark.als.TrainConfig;

public class StreamingCommand implements Command {
    private Engine engine;

    @Inject
    public StreamingCommand(Engine engine) {
        this.engine = engine;
    }

    @Override
    public void execute() {
        TrainConfig trainConfig = new TrainConfig(10, 4);
        engine.start(trainConfig);
        engine.test();
    }

    @Override
    public String getName() {
        return Command.DEFAULT_COMMAND;
    }
}
