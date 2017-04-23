package com.movierecommender.main.model;

import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;

public class RawRating implements Serializable {
    private int user;
    private int product;
    private double rating;

    public RawRating(int user, int product, double rating) {
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

    public double getRating() {
        return rating;
    }

    public Rating toSparkRating() {
        return new Rating(user, product, rating);
    }

    public static RawRating fromSparkRating(Rating rating) {
        return new RawRating(rating.user(), rating.product(), rating.rating());
    }
}
