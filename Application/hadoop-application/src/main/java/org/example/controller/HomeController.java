package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

@Controller
@RequiredArgsConstructor
public class HomeController {

    private final FileSystem fileSystem;

    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("message", "안녕, HDFS!");
        return "index";
    }

    private String[] getHDFSFileList() { // HDFS 파일 목록 가져오는 메서드
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/uploaded")); // 업로드 디렉토리의 파일 목록 조회
            return Arrays.stream(fileStatuses)
                    .map(FileStatus::getPath) // 각 파일의 경로를 가져옴
                    .map(Path::getName) // 파일 이름만 추출
                    .toArray(String[]::new); // 배열로 변환
        } catch (IOException e) {
            e.printStackTrace();
            return new String[0]; // 오류 발생 시 빈 배열 반환
        }
    }
}