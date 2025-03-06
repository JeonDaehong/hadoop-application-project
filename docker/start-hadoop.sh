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
    <!-- WebHDFS 활성화 -->
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
    <!-- 클라이언트가 호스트이름 사용 설정 -->
    <property>
        <name>dfs.client.use.datanode.hostname</name>
        <value>false</value>
    </property>
    <!-- 데이터노드 호스트이름 사용 설정 -->
    <property>
        <name>dfs.datanode.use.datanode.hostname</name>
        <value>false</value>
    </property>
    <!-- 데이터노드 주소 바인딩 설정 -->
    <property>
        <name>dfs.datanode.address</name>
        <value>0.0.0.0:9866</value>
    </property>
    <!-- 데이터노드 HTTP 주소 -->
    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:9864</value>
    </property>
    <!-- 데이터노드 IPC 주소 -->
    <property>
        <name>dfs.datanode.ipc.address</name>
        <value>0.0.0.0:9867</value>
    </property>
    <!-- 호스트 이름 검사 비활성화 -->
    <property>
        <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
        <value>false</value>
    </property>
    <!-- 데이터노드 헬스 체크 간격 증가 -->
    <property>
        <name>dfs.namenode.heartbeat.recheck-interval</name>
        <value>300000</value>
    </property>
    <!-- 블록 보고서 간격 증가 -->
    <property>
        <name>dfs.blockreport.intervalMsec</name>
        <value>21600000</value>
    </property>
    <!-- 클라이언트 실패 후 재시도 간격 증가 -->
    <property>
        <name>dfs.client.failover.sleep.base.millis</name>
        <value>1000</value>
    </property>
    <!-- 데이터노드 호스트 이름 지정 (각 노드마다 수정 필요) -->
    <property>
        <name>dfs.datanode.hostname</name>
        <value>localhost</value>
    </property>
    <property>
        <name>dfs.namenode.safemode.extension</name>
        <value>0</value>
    </property>
    
    <!-- IP 주소 대신 호스트 주소 사용 방지 -->
    <property>
        <name>dfs.client.use.datanode.hostname</name>
        <value>false</value>
    </property>
    <!-- 블록 위치 정보에 호스트 주소 대신 IP 사용 -->
    <property>
        <name>dfs.datanode.use.datanode.hostname</name>
        <value>false</value>
    </property>
    <!-- 데이터노드 실패 처리 정책 -->
    <property>
        <name>dfs.client.block.write.replace-datanode-on-failure.policy</name>
        <value>NEVER</value>
    </property>
    <!-- 데이터노드 실패 시 교체 비활성화 -->
    <property>
        <name>dfs.client.block.write.replace-datanode-on-failure.enable</name>
        <value>false</value>
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
	if [ "$(hostname)" == "datanode1" ] || [ "$(hostname)" == "datanode2" ]; then
		# 데이터노드 호스트명 설정 수정
		CURRENT_HOSTNAME=$(hostname)
		echo "Setting datanode hostname to: $CURRENT_HOSTNAME"
		sed -i "s/<value>localhost<\/value>/<value>$CURRENT_HOSTNAME<\/value>/" ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml

		# 클러스터 ID 파일만 삭제 (데이터는 보존)
		if [ -f "${HADOOP_HOME}/data/datanode/current/VERSION" ]; then
			echo "Removing cluster ID information..."
			rm -f ${HADOOP_HOME}/data/datanode/current/VERSION
		fi
		
		echo "Starting DataNode..."
		${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode
	fi
    
    echo "Starting NodeManager..."
    ${HADOOP_HOME}/sbin/yarn-daemon.sh start nodemanager
    
    # 무한 대기
    tail -f /dev/null
fi