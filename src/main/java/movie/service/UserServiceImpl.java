package movie.service;

import movie.model.User;
import org.springframework.stereotype.Service;

/**
 * Created by Naks on 02-May-16.
 * Implementation of user service
 */

@Service
public class UserServiceImpl implements UserService{

    @Override
    public String getUserStatus(String userId){
        User user=new User();
        user.setUserId(Integer.parseInt(userId));
        return user.getStatus();
    }
}
