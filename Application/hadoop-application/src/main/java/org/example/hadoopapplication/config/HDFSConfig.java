package org.example.hadoopapplication.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HDFSConfig {

    @Bean
    public Configuration hadoopConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.datanode.use.datanode.hostname", "true");

        configuration.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));

        // 이 설정은 HDFS 클라이언트가 short-circuit read 를 사용할지 여부를 결정
        // 클라이언트와 DataNode 가 동일한 머신에 있을 때 성능을 향상시킬 수 있음.
        configuration.setBoolean("dfs.client.read.shortcircuit", false);

        // 이 설정은 short-circuit read 시에 데이터의 checksum 검사를 건너뛸지 여부를 설정
        // Checksum 은 데이터 무결성을 확인하기 위해 사용
        // Checksum 을 건너뛰면 성능을 향상시킬 수 있음.
        configuration.set("dfs.client.read.shortcircuit.skip.checksum", "true");
        configuration.setInt("dfs.client.block.reader.rpc.max.retries", 10);

        return configuration;
    }
}