package com.movierecommender.spark.model;

import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;

public class RawRating implements Serializable {
    private int user;
    private int product;
    private float rating;

    public RawRating(int user, int product, float rating) {
        this.user = user;
        this.product = product;
        this.rating = rating;
    }

    public int getUser() {
        return user;
    }

    public int getProduct() {
        return product;
    }

    public float getRating() {
        return rating;
    }

    public Rating toSparkRating() {
        return new Rating(user, product, rating);
    }
}
