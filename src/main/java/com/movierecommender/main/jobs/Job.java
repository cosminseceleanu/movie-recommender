package com.movierecommender.main.jobs;

public interface Job {
    String DEFAULT_COMMAND = "streaming";

    void execute();
    String getName();
}
