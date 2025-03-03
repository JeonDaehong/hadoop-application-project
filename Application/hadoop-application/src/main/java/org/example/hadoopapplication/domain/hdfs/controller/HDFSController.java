package org.example.hadoopapplication.domain.hdfs.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.hadoopapplication.domain.hdfs.service.HDFSService;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@RestController
@RequestMapping("/api/hdfs")
@RequiredArgsConstructor
@Slf4j
public class HDFSController {

    private final HDFSService hdfsService;

    @GetMapping("/download")
    public ResponseEntity<byte[]> downloadFileAlt(@RequestParam("fileName") String fileName) throws IOException {

        String hdfsPath = "/user/test/" + fileName;

        log.info("Downloading file: {}", hdfsPath);

        // 파일 내용을 메모리에 저장
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        hdfsService.downloadFile(hdfsPath, outputStream);
        byte[] fileContent = outputStream.toByteArray();

        // 응답 헤더 설정
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.setContentDispositionFormData("attachment", fileName);

        return ResponseEntity.ok()
                .headers(headers)
                .body(fileContent);
    }


    // 파일 업로드 엔드포인트 (참고용)
    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file,
                                             @RequestParam("path") String hdfsPath) throws IOException {
        try (InputStream inputStream = file.getInputStream()) {
            hdfsService.uploadFile(inputStream, hdfsPath);
            return ResponseEntity.ok("File uploaded successfully to: " + hdfsPath);
        }
    }

}
