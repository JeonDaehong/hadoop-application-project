package org.example.hadoopapplication.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HDFSConfig {

    @Bean
    public FileSystem fileSystem() throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        return FileSystem.get(conf);
    }
}