package movie.model;

import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Created by Naks on 02-May-16.
 * Payload properties for user type=BA
 */

@Component
public class TypeParser implements Serializable{

    private String type;
    private String movieName;
    private double rating;

    public TypeParser() {
    }

    public TypeParser(String type, String movieName, double rating) {
        this.type = type;
        this.movieName = movieName;
        this.rating = rating;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }
}
