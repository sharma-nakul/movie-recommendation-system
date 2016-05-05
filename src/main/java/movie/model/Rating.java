package movie.model;

import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Created by Naks on 02-May-16.
 * Movies that user rated.
 */

@Component
public class Rating implements Serializable{

    private int userId;
    private int movieId;
    private float ratingGivenByUser;

    public Rating() {
    }

    public Rating(int userId, int movieId, float ratingGivenByUser) {
        this.userId = userId;
        this.movieId = movieId;
        this.ratingGivenByUser = ratingGivenByUser;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public float getRatingGivenByUser() {
        return ratingGivenByUser;
    }

    public void setRatingGivenByUser(float ratingGivenByUser) {
        this.ratingGivenByUser = ratingGivenByUser;
    }
}
