package org.example.hadoop_app.config;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class HadoopConfig {
    @Bean
    public Configuration hadoopConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");

        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("dfs.datanode.use.datanode.hostname", "true");

        configuration.setBoolean("dfs.client.read.shortcircuit", false);
        configuration.set("dfs.client.read.shortcircuit.skip.checksum", "true");

        configuration.setInt("dfs.client.block.reader.rpc.max.retries", 10);

        return configuration;
    }
}
