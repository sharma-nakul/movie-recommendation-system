package movie.model;

import java.io.Serializable;

/**
 * Created by Naks on 04-May-16.
 * POJO Model of Pearson Correlation Algorithm
 */
public class PCModel implements Serializable{
    private int movieId;
    private float rating;

    public PCModel() {
    }

    public PCModel(int movieId, float rating) {
        this.movieId = movieId;
        this.rating = rating;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public float getRating() {
        return rating;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }
}
