#!/bin/bash

# SSH 시작
service ssh start

# 호스트 이름 설정
echo "$(hostname -I | awk '{print $1}') $(hostname)" >> /etc/hosts

# Hadoop 설정 파일 업데이트
# core-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/core-site.xml << XML
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
</configuration>
XML

# hdfs-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml << XML
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/opt/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/hadoop/data/datanode</value>
    </property>
    <!-- 클라이언트가 호스트 이름 대신 IP 주소 사용 설정 -->
    <property>
        <name>dfs.client.use.datanode.hostname</name>
        <value>true</value>
    </property>
    <!-- 데이터노드가 호스트 이름 대신 IP 주소 사용 설정 -->
    <property>
        <name>dfs.datanode.use.datanode.hostname</name>
        <value>true</value>
    </property>
    <!-- WebHDFS 활성화 -->
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <!-- 호스트 이름 체크 비활성화 -->
    <property>
        <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
        <value>false</value>
    </property>
    <!-- 데이터노드가 클라이언트에게 제공하는 데이터 전송 주소 -->
    <property>
        <name>dfs.datanode.address</name>
        <value>0.0.0.0:9866</value>
    </property>
    <!-- datanode2의 특수 설정 -->
    <property>
        <name>dfs.datanode.address.datanode2</name>
        <value>localhost:9867</value>
    </property>
    <!-- 클라이언트가 데이터노드에 연결할 때 사용할 외부 IP 지정 -->
    <property>
        <name>dfs.client.datanode.address.resolution.enabled</name>
        <value>true</value>
    </property>
</configuration>
XML

# mapred-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/mapred-site.xml << XML
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>\${HADOOP_HOME}/share/hadoop/mapreduce/*:\${HADOOP_HOME}/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
XML

# yarn-site.xml
cat > ${HADOOP_HOME}/etc/hadoop/yarn-site.xml << XML
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>resourcemanager</value>
    </property>
</configuration>
XML

# workers 파일 설정
cat > ${HADOOP_HOME}/etc/hadoop/workers << WORKERS
datanode1
datanode2
WORKERS

# 디렉토리 생성
mkdir -p ${HADOOP_HOME}/data/namenode ${HADOOP_HOME}/data/datanode

# 컨테이너 역할에 따라 다른 작업 수행
if [ "$(hostname)" == "namenode" ]; then
    # 포맷 여부를 확인하는 플래그 파일 경로
    FORMAT_FLAG="${HADOOP_HOME}/data/namenode/format_complete"
    
    # 플래그 파일이 없으면 처음 실행으로 판단하고 포맷 수행
    if [ ! -f "$FORMAT_FLAG" ]; then
        echo "First time running, formatting namenode..."
        echo "Y" | ${HADOOP_HOME}/bin/hdfs namenode -format
        # 포맷 완료 플래그 생성
        touch "$FORMAT_FLAG"
    else
        echo "NameNode already formatted, skipping format..."
    fi
    
    echo "Starting namenode..."
    ${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode

    # 무한 대기
    tail -f /dev/null
elif [ "$(hostname)" == "resourcemanager" ]; then
    echo "Starting ResourceManager..."
    ${HADOOP_HOME}/sbin/yarn-daemon.sh start resourcemanager
    
    # 무한 대기
    tail -f /dev/null
else
    echo "Starting DataNode..."
    ${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode
    
    echo "Starting NodeManager..."
    ${HADOOP_HOME}/sbin/yarn-daemon.sh start nodemanager
    
    # 무한 대기
    tail -f /dev/null
fi
