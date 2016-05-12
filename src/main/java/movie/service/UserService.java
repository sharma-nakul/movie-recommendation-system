package movie.service;

import movie.model.TypeParser;
import movie.model.User;

import java.util.List;

/**
 * Created by Naks on 02-May-16.
 * Interface for user service
 */
public interface UserService {

    User getUserStatus(String userId);
    List<TypeParser> generateRecommendation (String type, int userId);
}
