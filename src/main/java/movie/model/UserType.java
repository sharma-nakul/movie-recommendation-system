package movie.model;

import java.io.Serializable;

/**
 * Created by Naks on 2-May-16.
 * Converting into JSON
 */
public class UserType implements Serializable{
    private int userId;
    private String type;

    public UserType() {
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
