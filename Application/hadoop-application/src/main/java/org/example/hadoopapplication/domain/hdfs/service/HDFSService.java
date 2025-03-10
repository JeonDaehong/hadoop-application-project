package org.example.hadoopapplication.domain.hdfs.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class HDFSService {

    private final Configuration hadoopConfiguration;

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

            // HDFS 에서 파일 읽기
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
        try (FileSystem fileSystem = FileSystem.get(hadoopConfiguration)) {
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
        }
    }

    // HDFS 파일 목록 가져오는 메서드
    public String[] getHDFSFileList() {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(hadoopConfiguration);
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
}
