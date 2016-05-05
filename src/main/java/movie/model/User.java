package movie.model;

/**
 * Created by Naks on 02-May-16.
 * Read user who has logged in
 */
public class User {
    private int userId;

    public User() {
        this.userId = 1;

    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }
}
