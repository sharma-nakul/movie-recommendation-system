package movie.model;

import java.io.Serializable;

/**
 * Created by Naks on 03-May-16.
 * POJO Class for movie recommendations.
 */

public class MovieRecommendation implements Serializable {

    private int movieId;
    private String movieName;
    private float recoValue;

    public MovieRecommendation(int movieId, String movieName, float recoValue) {
        this.movieId = movieId;
        this.movieName = movieName;
        this.recoValue = recoValue;
    }

    public MovieRecommendation() {
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

    public float getRecoValue() {
        return recoValue;
    }

    public void setRecoValue(float recoValue) {
        this.recoValue = recoValue;
    }
}
