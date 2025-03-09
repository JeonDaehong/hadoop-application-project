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


    public List<Map<String, Object>> getDataNodesInfo() {
        FileSystem fileSystem = null;
        DFSClient dfsClient;

        try {
            fileSystem = FileSystem.get(hadoopConfiguration);
            dfsClient = new DFSClient(fileSystem.getUri(), hadoopConfiguration);

            DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;
            List<Map<String, Object>> dataNodeInfoList = new ArrayList<>();

            // 모든 데이터 노드 정보 가져오기
            DatanodeInfo[] dataNodes = dfsClient.datanodeReport(HdfsConstants.DatanodeReportType.LIVE);

            // /user/test 디렉토리 내 파일 목록 가져오기
            Path dirPath = new Path("/user/test/");  // 절대 경로로 수정
            FileStatus[] fileStatuses = fileSystem.listStatus(dirPath);

            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {  // 파일인 경우만 처리
                    String filePath = fileStatus.getPath().toString();
                    Path fileHdfsPath = new Path(filePath);  // Path 객체로 경로 설정

                    // 해당 파일의 블록 정보 조회
                    // 경로가 절대경로인지 확인하고, 문제가 있을 경우 경로를 추가
                    if (!fileHdfsPath.isAbsolute()) {
                        fileHdfsPath = new Path(fileSystem.getUri().toString() + filePath);
                    }

                    LocatedBlocks locatedBlocks = dfs.getClient().getNamenode().getBlockLocations(fileHdfsPath.toString(), 0, Long.MAX_VALUE);

                    for (DatanodeInfo dataNode : dataNodes) {
                        Map<String, Object> dataNodeInfo = new HashMap<>();
                        dataNodeInfo.put("hostName", dataNode.getHostName());
                        dataNodeInfo.put("ipAddr", dataNode.getIpAddr());
                        dataNodeInfo.put("lastHeartbeat", dataNode.getLastUpdate());
                        dataNodeInfo.put("capacity", dataNode.getCapacity());
                        dataNodeInfo.put("usedSpace", dataNode.getDfsUsed());
                        dataNodeInfo.put("remainingSpace", dataNode.getRemaining());

                        // 해당 데이터 노드가 가진 블록 정보 조회
                        List<Map<String, Object>> blockInfoList = new ArrayList<>();
                        for (LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
                            for (int i = 0; i < block.getLocations().length; i++) {
                                if (block.getLocations()[i].getIpAddr().equals(dataNode.getIpAddr())) {
                                    Map<String, Object> blockInfo = new HashMap<>();
                                    blockInfo.put("blockId", block.getBlock().getBlockId());
                                    blockInfo.put("blockSize", block.getBlockSize());
                                    blockInfo.put("isReplication", block.isCorrupt() ? "No" : "Yes"); // 복제 여부 확인
                                    blockInfoList.add(blockInfo);
                                }
                            }
                        }

                        dataNodeInfo.put("blocks", blockInfoList);
                        dataNodeInfoList.add(dataNodeInfo);
                    }
                }
            }

            return dataNodeInfoList;

        } catch (IOException e) {
            log.error("데이터 노드 정보를 가져오는 데 실패했습니다.", e);
            return new ArrayList<>();
        } finally {
            try {
                if (fileSystem != null) {
                    fileSystem.close();
                }
            } catch (IOException e) {
                log.error("파일 시스템을 닫는 데 실패했습니다.", e);
            }
        }
    }
}
