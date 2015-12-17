package com.udemy.hackathon.common;

import com.udemy.hackathon.domain.Movie;
import com.udemy.hackathon.domain.UserRating;
import org.apache.spark.mllib.recommendation.Rating;

public class DataParser {

    private static final String SEPARATOR = "::";

    private DataParser() {
    }

    public static Movie parseToMovie(String movieString) {
        String[] parts = movieString.split(SEPARATOR);
        return new Movie(Integer.parseInt(parts[0]), parts[1], parts[2]);
    }

    public static UserRating parseToUserRating(String userRatingString) {
        String[] parts = userRatingString.split(SEPARATOR);
        return new UserRating(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
                Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
    }

    public static Rating parseToRating(String ratingString) {
        String[] parts = ratingString.split(SEPARATOR);
        return new Rating(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
                Double.parseDouble(parts[2]));
    }
}
