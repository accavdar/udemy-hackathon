package com.udemy.hackathon.domain;

public class UserRating {

    private int userId;

    private int movieId;

    private double rating;

    private int timestamp;

    public UserRating(int userId, int movieId, double rating, int timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public double getUserRating() {
        return rating;
    }

    public int getTimestamp() {
        return timestamp;
    }
}
