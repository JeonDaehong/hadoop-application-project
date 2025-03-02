package org.example.hadoopapplication.domain.hdfs.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class HDFSService {

    private final FileSystem fileSystem;

    // HDFS 파일 목록 가져오는 메서드
    public String[] getHDFSFileList() {
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/test"));
            return Arrays.stream(fileStatuses)
                    .map(FileStatus::getPath)
                    .map(Path::getName)
                    .toArray(String[]::new);
        } catch (IOException e) {
            log.error("ERROR >>> {}", e.getMessage());
            return new String[0];
        }
    }

    public String uploadFile(MultipartFile file, Model model) {
        try {
            Path hdfsPath = new Path("/user/test/" + file.getOriginalFilename());
            try (InputStream inputStream = file.getInputStream()) {
                fileSystem.copyFromLocalFile(new Path(Objects.requireNonNull(file.getOriginalFilename())), hdfsPath);
                return "파일이 HDFS에 성공적으로 업로드되었습니다: " + file.getOriginalFilename();
            }
        } catch (IOException e) {
            return "파일 업로드 중 오류가 발생했습니다: " + e.getMessage();
        }
    }

    public InputStreamResource downloadFile(@RequestParam String filename) {
        try {
            Path hdfsPath = new Path("/user/test/" + filename);
            InputStream inputStream = fileSystem.open(hdfsPath);
            return new InputStreamResource(inputStream);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
