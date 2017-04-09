package com.movierecommender.main.commands;

public interface Command {
    public final static String DEFAULT_COMMAND = "spark-streaming";

    void execute();
    String getName();
}
