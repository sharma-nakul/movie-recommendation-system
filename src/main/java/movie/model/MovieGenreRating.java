package movie.model;

/**
 * Created by Shounak on 5/2/2016.
 */
public class MovieGenreRating {
    String genreName;
    double averageRating;
    String movieName;

    public MovieGenreRating(String genreName, double averageRating, String movieName) {
        this.movieName = movieName;
        this.genreName = genreName;
        this.averageRating = averageRating;
    }

    public String getGenreName() {
        return genreName;
    }

    public void setGenreName(String genreName) {
        this.genreName = genreName;
    }

    public double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(double averageRating) {
        this.averageRating = averageRating;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }
}