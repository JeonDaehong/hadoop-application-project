package org.example.hadoopapplication.config;

import jakarta.servlet.MultipartConfigElement;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;

@Configuration
public class FileUploadConfig {

    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        factory.setMaxFileSize(DataSize.ofGigabytes(2));  // 단일 파일 최대 크기 2GB
        factory.setMaxRequestSize(DataSize.ofGigabytes(2)); // 전체 요청 크기 2GB
        return factory.createMultipartConfig();
    }
}