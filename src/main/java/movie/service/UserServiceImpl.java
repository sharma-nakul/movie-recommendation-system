package movie.service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import movie.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Naks on 02-May-16.
 * Implementation of user service
 */

@Service
public class UserServiceImpl implements UserService{

    @Autowired
    Session session;

    @Override
    public User getUserStatus(String userId){

        User user =new User();

        final ResultSet rows=session.execute("SELECT * from user_info");

        for(Row row :rows.all()){
            user.setUserId(row.getInt("user_id"));
            user.setMovieCount(row.getInt("movie_count"));
            if(user.getUserId()==Integer.valueOf(userId)) {
                user.setStatus("success");
                break;
            }
        }
        return user;
    }
}
