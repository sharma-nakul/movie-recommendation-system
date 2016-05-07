package movie.controller;

import movie.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;


@Controller
@RequestMapping(produces = {"application/json","text/html"})
public class WebController {

    @Autowired
    UserService userService;


    @ResponseStatus(HttpStatus.OK)
    @RequestMapping(value = "/login", method = RequestMethod.POST)
    public ResponseEntity getLogin(@RequestBody String userId) {

             String status=userService.getUserStatus(userId);
            return new ResponseEntity<String>(status,HttpStatus.OK);
    }

}