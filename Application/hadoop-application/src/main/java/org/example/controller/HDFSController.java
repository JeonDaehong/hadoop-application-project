package org.example.controller;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

@Controller
@RequiredArgsConstructor
public class HDFSController {

    private final FileSystem fileSystem;

    @PostMapping("/upload")
    public String uploadFile(MultipartFile file, Model model) {
        try {
            Path hdfsPath = new Path("/uploaded/" + file.getOriginalFilename());
            try (InputStream inputStream = file.getInputStream()) {
                fileSystem.copyFromLocalFile(new Path(Objects.requireNonNull(file.getOriginalFilename())), hdfsPath);
                model.addAttribute("message", "파일이 HDFS에 성공적으로 업로드되었습니다: " + file.getOriginalFilename());
            }
        } catch (IOException e) {
            model.addAttribute("message", "파일 업로드 중 오류가 발생했습니다: " + e.getMessage());
        }
        return "index";
    }

    @GetMapping("/download") // 파일 다운로드 메서드
    public ResponseEntity<InputStreamResource> downloadFile(@RequestParam String filename) {
        try {
            Path hdfsPath = new Path("/uploaded/" + filename);
            InputStream inputStream = fileSystem.open(hdfsPath);
            InputStreamResource resource = new InputStreamResource(inputStream);
            HttpHeaders headers = new HttpHeaders();
            headers.add("Content-Disposition", "attachment; filename=" + filename);
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(resource);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
    }
}
