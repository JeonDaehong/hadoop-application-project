package org.example.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.util.Arrays;

@Controller
@RequiredArgsConstructor
@Slf4j
public class HomeController {

    private final FileSystem fileSystem;

    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("message", "안녕, HDFS!");
        model.addAttribute("fileList", getHDFSFileList());
        return "index";
    }

    // HDFS 파일 목록 가져오는 메서드
    private String[] getHDFSFileList() {
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/uploaded"));
            return Arrays.stream(fileStatuses)
                    .map(FileStatus::getPath)
                    .map(Path::getName)
                    .toArray(String[]::new);
        } catch (IOException e) {
            log.error("ERROR >>> {}", e.getMessage());
            return new String[0];
        }
    }
}