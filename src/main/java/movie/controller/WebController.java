package movie.controller;

import movie.model.TypeParser;
import movie.model.User;
import movie.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @RequestMapping(value="/user", method = RequestMethod.GET)
    public ResponseEntity checkUserType(
            @RequestParam(value = "type", required = true) String type,
            @RequestParam(value="userId",required = true) String userId){

        try {
            List<TypeParser> results = userService.generateRecommendation(type, userId);
            return new ResponseEntity<List<TypeParser>>(results,HttpStatus.OK);
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("Exception -> /user method: "+e.getMessage());
            return new ResponseEntity(HttpStatus.BAD_REQUEST);
        }
    }

}