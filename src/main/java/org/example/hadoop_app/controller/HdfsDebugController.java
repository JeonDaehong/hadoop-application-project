package org.example.hadoop_app.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/hdfs/debug")
public class HdfsDebugController {

    private final Configuration hadoopConfiguration;

    @Autowired
    public HdfsDebugController(Configuration hadoopConfiguration) {
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @GetMapping("/test-connection")
    public Map<String, Object> testConnection() {
        Map<String, Object> result = new HashMap<>();
        FileSystem fileSystem = null;

        try {
            // 설정 정보 기록
            result.put("fs.defaultFS", hadoopConfiguration.get("fs.defaultFS"));
            result.put("dfs.client.use.datanode.hostname", hadoopConfiguration.get("dfs.client.use.datanode.hostname"));
            result.put("HADOOP_USER_NAME", System.getProperty("HADOOP_USER_NAME"));

            // 연결 시도
            fileSystem = FileSystem.get(hadoopConfiguration);
            result.put("connection", "success");

            // 루트 디렉토리 확인
            boolean exists = fileSystem.exists(new Path("/"));
            result.put("root_exists", exists);

            // 시스템 정보
            result.put("uri", fileSystem.getUri().toString());
            result.put("working_directory", fileSystem.getWorkingDirectory().toString());

            try {
                // 디렉토리 목록 가져오기 시도
                org.apache.hadoop.fs.FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
                String[] directories = new String[statuses.length];
                for (int i = 0; i < statuses.length; i++) {
                    directories[i] = statuses[i].getPath().getName();
                }

                result.put("directories", directories);
            } catch (Exception e) {
                result.put("list_error", e.getMessage());
            }

        } catch (Exception e) {
            result.put("connection", "failed");
            result.put("error", e.getMessage());
            result.put("error_type", e.getClass().getName());

            Throwable cause = e.getCause();
            if (cause != null) {
                result.put("cause", cause.getMessage());
                result.put("cause_type", cause.getClass().getName());
            }
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    result.put("close_error", e.getMessage());
                }
            }
        }

        return result;
    }
}
