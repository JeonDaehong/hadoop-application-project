package org.example.hadoop_app.controller;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

@RestController
@RequestMapping("/api/webhdfs")
public class WebHdfsController {

    @GetMapping("/download")
    public ResponseEntity<byte[]> downloadFile(@RequestParam("path") String hdfsPath) throws IOException {
        // WebHDFS API URL 구성 (기본 포트: 9870)
        String webHdfsUrl = "http://localhost:9870/webhdfs/v1" + hdfsPath + "?op=OPEN";

        // HTTP 연결 설정
        URL url = new URL(webHdfsUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // 응답 코드 확인
        if (conn.getResponseCode() != 200) {
            throw new IOException("Failed to open file: " + conn.getResponseMessage());
        }

        // 응답 내용 읽기
        try (InputStream is = conn.getInputStream()) {
            byte[] content = is.readAllBytes();

            // 파일 이름 추출
            String fileName = hdfsPath.substring(hdfsPath.lastIndexOf('/') + 1);

            // HTTP 응답 헤더 설정
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.setContentDispositionFormData("attachment", fileName);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(content);
        }
    }

    @GetMapping("/list")
    public ResponseEntity<String> listDirectory(@RequestParam("path") String hdfsPath) throws IOException {
        // WebHDFS API URL 구성 - LISTSTATUS 작업
        String webHdfsUrl = "http://localhost:9870/webhdfs/v1" + hdfsPath + "?op=LISTSTATUS";

        // HTTP 연결 설정
        URL url = new URL(webHdfsUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // 응답 코드 확인
        if (conn.getResponseCode() != 200) {
            throw new IOException("Failed to list directory: " + conn.getResponseMessage());
        }

        // 응답 내용 읽기
        try (InputStream is = conn.getInputStream()) {
            byte[] content = is.readAllBytes();
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new String(content));
        }
    }
}
