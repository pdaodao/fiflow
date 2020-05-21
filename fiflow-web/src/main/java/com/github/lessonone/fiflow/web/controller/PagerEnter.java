package com.github.lessonone.fiflow.web.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PagerEnter {

    @GetMapping({"", "/index"})
    public String index() {
        return "/static/index.html";
    }
}
