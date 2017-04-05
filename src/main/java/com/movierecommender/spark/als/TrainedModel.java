package com.movierecommender.spark.als;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

public class TrainedModel {
    private final double error;
    private final MatrixFactorizationModel model;

    public TrainedModel(double error, MatrixFactorizationModel model) {
        this.error = error;
        this.model = model;
    }

    public double getError() {
        return error;
    }

    public MatrixFactorizationModel getModel() {
        return model;
    }
}
