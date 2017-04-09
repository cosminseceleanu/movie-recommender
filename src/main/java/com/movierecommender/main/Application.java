package com.movierecommender.main;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.movierecommender.main.di.SparkModule;
import com.movierecommender.spark.Engine;
import com.movierecommender.spark.als.TrainConfig;

public class Application {
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new SparkModule());

        Engine engine = injector.getInstance(Engine.class);
        TrainConfig trainConfig = new TrainConfig(10, 4);
        engine.start(trainConfig);
        engine.test();
    }
}
