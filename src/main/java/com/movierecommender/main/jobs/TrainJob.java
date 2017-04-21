package com.movierecommender.main.jobs;


public class TrainJob implements Job {
    @Override
    public void execute() {

    }

    @Override
    public String getName() {
        return "spark-train";
    }
}
