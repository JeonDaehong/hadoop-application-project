package org.example.hadoopapplication.domain.hdfs.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.hadoopapplication.domain.hdfs.service.HDFSService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
@Slf4j
public class HomeController {

    private final HDFSService hdfsService;

    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("message", "안녕, HDFS!");
        model.addAttribute("fileList", hdfsService.getHDFSFileList());
        return "index";
    }


}