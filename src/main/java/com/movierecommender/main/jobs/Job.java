package com.movierecommender.main.jobs;

public interface Job {
    public final static String DEFAULT_COMMAND = "spark-streaming";

    void execute();
    String getName();
}
