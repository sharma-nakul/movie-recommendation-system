package movie.controller;

import movie.model.User;
import movie.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
public class WebController {

    @Autowired
    UserService userService;


    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public ResponseEntity getLogin(@RequestBody String userId) {

        User user=userService.getUserStatus(userId);
            return new ResponseEntity<User>(user,HttpStatus.OK);
    }

}