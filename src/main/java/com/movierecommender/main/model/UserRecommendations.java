package com.movierecommender.main.model;

import java.util.Set;

public class UserRecommendations {
    private final int user;
    private final Set<Integer> products;

    public UserRecommendations(int user, Set<Integer> products) {
        this.user = user;
        this.products = products;
    }

    public int getUser() {
        return user;
    }

    public Set<Integer> getProducts() {
        return products;
    }
}
