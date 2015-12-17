package com.udemy.hackathon.recommendation;

import com.udemy.hackathon.common.DataParser;
import com.udemy.hackathon.domain.Movie;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class MovieLensALS {

    private static final Logger LOGGER = Logger.getLogger(MovieLensALS.class.getName());

    private static final int NUMBER_OF_PARTIONS = 4;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: MovieLensALS <dataDir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("MovieLensALS");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> ratingLines = context.textFile(args[0] + "/ratings.dat");
        JavaRDD<String> moviesLines = context.textFile(args[0] + "/movies.dat");
        JavaRDD<String> personalLines = context.textFile(args[0] + "/personalRatings.dat");

        JavaPairRDD<Integer, Rating> ratings = ratingLines
                .map(s -> DataParser.parseToUserRating(s))
                .mapToPair(r -> new Tuple2<>(r.getTimestamp() % 10,
                        new Rating(r.getUserId(), r.getMovieId(), r.getUserRating())));

        JavaPairRDD<Integer, Movie> movies = moviesLines
                .map(s -> DataParser.parseToMovie(s))
                .mapToPair(m -> new Tuple2<>(m.getMovieId(), m));

        JavaRDD<Rating> personalRatings = personalLines
                .map(s -> DataParser.parseToRating(s));

        long numOfRating = ratings.count();
        long numOfUsers = ratings.map(r -> r._2().user()).distinct().count();
        long numOfMovies = ratings.map(r -> r._2().product()).distinct().count();

        JavaRDD<Rating> training = ratings
                .filter(r -> r._1() < 6)
                .values()
                .union(personalRatings)
                .repartition(NUMBER_OF_PARTIONS)
                .cache();

        JavaRDD<Rating> validation = ratings
                .filter(r -> r._1() >= 6 && r._1() < 8)
                .values()
                .repartition(NUMBER_OF_PARTIONS)
                .cache();

        JavaRDD<Rating> test = ratings
                .filter(r -> r._1() >= 8)
                .values()
                .cache();

        long numOfTraining = training.count();
        long numOfValidation = validation.count();
        long numOfTest = test.count();

        List<Integer> ranks = Arrays.asList(8, 12);
        List<Double> lambdas = Arrays.asList(0.1, 10.0);
        List<Integer> numIters = Arrays.asList(10, 20);
        MatrixFactorizationModel bestModel = null;
        double bestValidationRmse = Double.MAX_VALUE;
        int bestRank = 0;
        double bestLambda = -1;
        int bestNumIter = -1;

        for (Integer rank : ranks) {
            for (Double lambda : lambdas) {
                for (Integer numIter : numIters) {
                    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(training), rank, numIter, lambda);
                    double validationRmse = computeRmse(model, validation, numOfValidation);
                    if (validationRmse < bestValidationRmse) {
                        bestModel = model;
                        bestValidationRmse = validationRmse;
                        bestRank = rank;
                        bestLambda = lambda;
                        bestNumIter = numIter;
                    }
                }
            }
        }

        double testRmse = computeRmse(bestModel, test, numOfTest);

        List<Integer> personalMovieIds = personalRatings.map(r -> r.product()).collect();
        JavaPairRDD<Integer, Integer> moviesNotWacthed = movies
                .map(m -> m._1())
                .filter(id -> !personalMovieIds.contains(id))
                .mapToPair(id -> new Tuple2<>(0, id));

        List<Rating> recommendations = bestModel.predict(moviesNotWacthed).sortBy(r -> r.rating(), false, 1).collect();

        context.stop();

        LOGGER.info("Ratings count: " + numOfRating);
        LOGGER.info("Users count: " + numOfUsers);
        LOGGER.info("Movies count: " + numOfMovies);

        LOGGER.info("Training count: " + numOfTraining);
        LOGGER.info("Validation count: " + numOfValidation);
        LOGGER.info("Test count: " + numOfTest);

        LOGGER.info("The best model was trained with rank = " + bestRank +
                " and lambda = " + bestLambda + ", and numIter = " + bestNumIter);
        LOGGER.info("RMSE on the test set is: " + testRmse);

        LOGGER.info("Movie Recommendations");
        LOGGER.info("Recommendation Count: " + recommendations.size());

        for (Rating r : recommendations.subList(0, 50)) {
            LOGGER.info("Movie: " + r.toString());
        }
    }

    private static double computeRmse(MatrixFactorizationModel model, JavaRDD<Rating> data, long n) {
        JavaRDD<Rating> predictions = model.predict(data.mapToPair(r -> new Tuple2<>(r.user(), r.product())));
        JavaRDD<Tuple2<Double, Double>> predictionsAndRatings = predictions
                .mapToPair(p -> new Tuple2<>(new Tuple2<>(p.user(), p.product()), p.rating()))
                .join(data.mapToPair(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .values();

        return Math.sqrt(predictionsAndRatings
                .map(x -> (x._1() - x._2()) * (x._1() - x._2()))
                .reduce((x, y) -> x + y) / n);
    }
}
