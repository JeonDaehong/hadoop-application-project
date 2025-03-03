package org.example.hadoop_app.controller;

import org.example.hadoop_app.service.HdfsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

@RestController
@RequestMapping("/api/hdfs")
public class HdfsController {

    private final HdfsService hdfsService;

    @Autowired
    public HdfsController(HdfsService hdfsService) {
        this.hdfsService = hdfsService;
    }

    @GetMapping("/download")
    public void downloadFile(@RequestParam("path") String hdfsPath,
                             HttpServletResponse response) throws IOException {
        // 파일 이름 추출
        String fileName = hdfsPath.substring(hdfsPath.lastIndexOf('/') + 1);

        // 응답 헤더 설정
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + fileName + "\"");

        // HDFS에서 파일 다운로드하여 응답으로 전송
        hdfsService.downloadFile(hdfsPath, response.getOutputStream());
    }

    // 파일 다운로드 (ResponseEntity 방식)
    @GetMapping("/download-alt")
    public ResponseEntity<byte[]> downloadFileAlt(@RequestParam("path") String hdfsPath) throws IOException {
        // 파일 이름 추출
        String fileName = hdfsPath.substring(hdfsPath.lastIndexOf('/') + 1);

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

    @GetMapping("/webhdfs-download")
    public ResponseEntity<byte[]> webhdfsDownload(@RequestParam("path") String hdfsPath) throws IOException {
        String webHdfsUrl = "http://localhost:9870/webhdfs/v1" + hdfsPath + "?op=OPEN";
        URL url = new URL(webHdfsUrl);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        if (conn.getResponseCode() != 200) {
            throw new IOException("Failed to open file: " + conn.getResponseMessage());
        }

        try (InputStream is = conn.getInputStream()) {
            byte[] content = is.readAllBytes();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            String fileName = hdfsPath.substring(hdfsPath.lastIndexOf('/') + 1);
            headers.setContentDispositionFormData("attachment", fileName);

            return ResponseEntity.ok().headers(headers).body(content);
        }
    }
}
