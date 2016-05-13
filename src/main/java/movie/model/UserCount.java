package movie.model;

/**
 * Created by Shounak on 5/2/2016.
 */
public class UserCount {
    int UserId;
    int count;

    public UserCount(int userId, int count) {
        this.UserId = userId;
        this.count = count;
    }

    public int getUserId() {
        return UserId;
    }

    public void setUserId(int userId) {
        UserId = userId;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}