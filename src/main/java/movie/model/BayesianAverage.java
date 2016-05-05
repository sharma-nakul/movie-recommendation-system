package movie.model;

import java.io.Serializable;

/**
 * Created by Naks on 02-May-16.
 * Bayesian Average model class
 */
public class BayesianAverage implements Serializable {

    private int movieId;
    private String movieName;
    private double averageRating;
    private int count;
    private double bayesianAverage;

    public BayesianAverage() {
    }

    public BayesianAverage(int movieId, String movieName, double averageRating, int count, double bayesianAverage) {
        this.movieId = movieId;
        this.movieName = movieName;
        this.averageRating = averageRating;
        this.count = count;
        this.bayesianAverage = bayesianAverage;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public double getBayesianAverage() {
        return bayesianAverage;
    }

    public void setBayesianAverage(double bayesianAverage) {
        this.bayesianAverage = bayesianAverage;
    }
}
