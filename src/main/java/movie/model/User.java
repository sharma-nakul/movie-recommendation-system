package movie.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;

/**
 * Created by Naks on 02-May-16.
 * Read user who has logged in
 */
public class User implements Serializable {
    private int userId;
    private String status;
    private int movieCount;
    private String type;

    public User() {
        this.userId = 1;
        this.status = "failure";
        this.movieCount = 0;
        type = "BA";
    }

    @JsonIgnore
    public int getMovieCount() {
        return movieCount;
    }

    public String getType() {
        if (this.movieCount == 0)
            type = "BA";
        //// TODO: 08-May-16 Include Weighted Average condition here
        else
            type = "PC";
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setMovieCount(int movieCount) {
        this.movieCount = movieCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @JsonIgnore
    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }
}
