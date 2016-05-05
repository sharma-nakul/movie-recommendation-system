package movie.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Naks on 02-May-16.
 * Movies list and their corresponding list of genre
 */
public class Movie implements Serializable {
    private int movieId;
    private String movieName;
    private List<String> genreList;

    public Movie(int movieId, String movieName, List<String> genreList) {
        this.movieId = movieId;
        this.movieName = movieName;
        this.genreList = genreList;
    }

    public Movie(List<String> genreList) {
        this.genreList = genreList;
    }

    public Movie() {
        genreList = new ArrayList<>();
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

    public List<String> getGenreList() {
        return genreList;
    }

    public void setGenreList(List<String> genreList) {
        this.genreList = genreList;
    }
}