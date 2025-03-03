package org.example.hadoop_app.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;

@Service
public class HdfsService {

    private final Configuration hadoopConfiguration;

    @Autowired
    public HdfsService(Configuration hadoopConfiguration) {
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public void downloadFile(String hdfsPath, OutputStream outputStream) throws IOException {
        FileSystem fileSystem = null;
        FSDataInputStream inputStream = null;
        try {
            fileSystem = FileSystem.get(hadoopConfiguration);

            // HDFS 파일 경로 생성
            Path path = new Path(hdfsPath);

            // 파일이 존재하는지 확인
            if (!fileSystem.exists(path)) {
                throw new IOException("File not found at path: " + hdfsPath);
            }

            // HDFS에서 파일 읽기
            inputStream = fileSystem.open(path);

            // 파일 내용을 출력 스트림으로 복사
            IOUtils.copyBytes(inputStream, outputStream, hadoopConfiguration, false);

        } finally {
            // 리소스 정리
            if (inputStream != null) {
                IOUtils.closeStream(inputStream);
            }
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    // HDFS에 파일 업로드 메서드 (참고용)
    public void uploadFile(java.io.InputStream inputStream, String hdfsPath) throws IOException {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(hadoopConfiguration);
            Path path = new Path(hdfsPath);

            // 부모 디렉터리가 없을 경우 생성
            Path parent = path.getParent();
            if (parent != null && !fileSystem.exists(parent)) {
                fileSystem.mkdirs(parent);
            }

            // 파일이 이미 존재하면 삭제
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, false);
            }

            // HDFS에 파일 쓰기
            try (org.apache.hadoop.fs.FSDataOutputStream outputStream = fileSystem.create(path)) {
                IOUtils.copyBytes(inputStream, outputStream, hadoopConfiguration, true);
            }
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }
}
