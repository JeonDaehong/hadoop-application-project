package org.example.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HDFSConfig {

    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        return FileSystem.get(configuration);
    }
}