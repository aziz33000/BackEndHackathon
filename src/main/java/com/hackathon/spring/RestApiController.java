package com.hackathon.spring;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/data")
public class RestApiController {

    // -------------------Retrieve header---------------------------------------------

    @RequestMapping(value = "/header/", method = RequestMethod.GET)
    public void getHeader(@PathVariable("file") String file) {

        return ;
    }
}
