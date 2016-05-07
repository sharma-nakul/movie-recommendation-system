package movie.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Created by Naks on 02-May-16.
 * Read user who has logged in
 */
public class User {
    private int userId;
    private String status;

    public User() {
        this.userId = 1;
        this.status="failure";
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
