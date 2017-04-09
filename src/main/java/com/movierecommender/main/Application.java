package com.movierecommender.main;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.movierecommender.main.commands.CommandExecutor;
import com.movierecommender.main.di.MainModule;
import com.movierecommender.main.di.SparkModule;

public class Application {
    private Args args;

    public static void main(String[] args) {
        Application application = new Application();
        application.parseArgs(args);

        Injector injector = Guice.createInjector(new MainModule(), new SparkModule());

        CommandExecutor executor = injector.getInstance(CommandExecutor.class);
        executor.execute("aaa");
    }

    private void parseArgs(String[] argv) {
        args = new Args();
        JCommander commander = new JCommander();
        commander.addObject(args);
        commander.parse(argv);
    }
}
