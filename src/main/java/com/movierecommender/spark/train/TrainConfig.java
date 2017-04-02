package com.movierecommender.spark.train;

public class TrainConfig {
    private final int iterationsNr;
    private final int rankNr;

    public TrainConfig(int iterationsNr, int rankNr) {
        this.iterationsNr = iterationsNr;
        this.rankNr = rankNr;
    }

    public int getIterationsNr() {
        return iterationsNr;
    }

    public int getRankNr() {
        return rankNr;
    }
}
