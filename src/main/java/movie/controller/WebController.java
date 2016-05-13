package movie.controller;

import com.google.gson.Gson;
import movie.model.MovieGenreRating;
import movie.model.TypeParser;
import movie.model.User;
import movie.model.UserType;
import movie.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
public class WebController {

    @Autowired
    UserService userService;

    private static final Logger logger = LoggerFactory.getLogger(WebController.class);


    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public ResponseEntity getLogin(@RequestBody String userId) {
        try {
            User user = userService.getUserStatus(userId);
            return new ResponseEntity<User>(user, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/user", method = RequestMethod.POST)
    public ResponseEntity checkUserType(@RequestBody String json) {
        try {
            logger.info("/user controller has been called.");
            Gson gson = new Gson();
            UserType user = gson.fromJson(json, UserType.class);

            int userId = user.getUserId();
            String type = user.getType();
            logger.info("User Id -> " + userId);
            logger.info("Type -> " + type);

            List<TypeParser> results = userService.generateRecommendation(type, userId);
            return new ResponseEntity<List<TypeParser>>(results, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Exception -> /user method: " + e.getMessage());
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/genre", method = RequestMethod.POST)
    public ResponseEntity getGenreCorrelation(@RequestBody String userId) {
        try {
            List<MovieGenreRating> results = userService.getGenreCorrelation(userId);
            return new ResponseEntity<List<MovieGenreRating>>(results, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Exception -> /genre method: " + e.getMessage());
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
    }
}