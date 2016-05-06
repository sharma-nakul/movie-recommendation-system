package movie.model;

/**
 * Created by Shounak on 5/5/2016.
 * Class to store 1-1 mapping of genres and movies
 */
public class MovieGenres {
    int movieId;
    String movieName;
    String Genre;
    public MovieGenres(int movieId, String movieName, String genre) {
        this.movieId = movieId;
        this.movieName = movieName;
        Genre = genre;
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

    public String getGenre() {
        return Genre;
    }

    public void setGenre(String genre) {
        Genre = genre;
    }

}
