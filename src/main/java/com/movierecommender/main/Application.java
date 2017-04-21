package com.movierecommender.main;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.movierecommender.main.jobs.Job;
import com.movierecommender.main.jobs.JobExecutor;
import com.movierecommender.main.di.MainModule;
import com.movierecommender.main.di.SparkModule;

public class Application {
    private static Args args;

    public static void main(String[] arguments) {
        parseArgs(arguments);
        Injector injector = Guice.createInjector(new MainModule(), new SparkModule());

        JobExecutor executor = injector.getInstance(JobExecutor.class);
        executor.execute(Job.DEFAULT_COMMAND);
    }

    private static void parseArgs(String[] arguments) {
        args = new Args();
        JCommander commander = new JCommander();
        commander.addObject(args);
        commander.parse(arguments);
    }
}
