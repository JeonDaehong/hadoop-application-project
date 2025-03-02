1. docker build -t hadoop-cluster .
2. mkdir -p ./data/namenode ./data/datanode1 ./data/datanode2
3. docker-compose up -d
4. docker ps
5. http://localhost:9864/
6. http://localhost:9865/
7. docker exec -it namenode bash
8. hdfs dfs -ls /
9. hdfs dfs -mkdir -p /user/test
10. echo "Hello Hadoop Distributed File System" > test.txt
11. hdfs dfs -put test.txt /user/test/
12. hdfs dfs -ls /user/test/
13. hdfs dfs -cat /user/test/test.txt
14. hdfs dfs -stat %r /user/test/test.txt
15. dd if=/dev/urandom of=bigfile.txt bs=1M count=100
16. hdfs dfs -put bigfile.txt /user/test/
17. hdfs fsck /user/test/bigfile.txt -files -blocks -locations