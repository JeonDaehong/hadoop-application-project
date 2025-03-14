package org.example.hadoopapplication.domain.hdfs.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RestController
@Slf4j
@RequestMapping("/api/iceberg")
@RequiredArgsConstructor
public class IcebergController {

    private final Configuration hadoopConfiguration;

    final String ICEBERG_TABLE_PATH = "/user/table/iceberg";

    @GetMapping("/create/table")
    public void createIcebergTable() {

        HadoopTables tables = new HadoopTables(hadoopConfiguration);

        // 테이블이 이미 존재하면 생성하지 않도록 처리
        if (tables.exists(ICEBERG_TABLE_PATH)) {
            log.info("Iceberg Table already exists at {}", ICEBERG_TABLE_PATH);
        } else {
            // 테이블 스키마 정의 (CSV 파일의 컬럼에 맞게 수정)
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "age", Types.IntegerType.get())
            );

            // 파티셔닝 스펙 (필요시 수정)
            PartitionSpec spec = PartitionSpec.unpartitioned();

            // 테이블 생성
            tables.create(schema, spec, ICEBERG_TABLE_PATH);

            log.info("Iceberg Table created at {}", ICEBERG_TABLE_PATH);
        }
    }

    @GetMapping("/put/data")
    public void putData() {

        // SparkSession 생성
        SparkSession sparkSession = SparkSession.builder()
                .appName("Iceberg Table Appender")  // Spark 애플리케이션 이름
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")  // Iceberg 카탈로그 설정
                .config("spark.sql.catalog.spark_catalog.type", "hadoop")  // Hadoop 카탈로그 설정
                .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://localhost:9000/user/test/iceberg")  // Iceberg 테이블 경로 (HDFS 경로 설정)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")  // Iceberg 확장 설정
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")  // HDFS 기본 파일 시스템 설정
                .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")  // HDFS 파일 시스템 구현 설정
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")  // 로컬 파일 시스템 구현 설정
                .getOrCreate();  // SparkSession 생성

        // 랜덤 데이터를 생성하는 부분
        Random random = new Random();

        List<Row> dummyDataList = new ArrayList<>();

        // 10개의 더미 데이터를 생성 (원하는 만큼 생성 가능)
        for (int i = 0; i < 10; i++) {
            int id = random.nextInt(1000); // 0~999 사이의 랜덤 아이디
            String name = "Name_" + id; // 랜덤 이름
            int age = random.nextInt(50) + 18; // 18~67 사이의 랜덤 나이
            // Row 객체로 변환 후 리스트에 추가
            dummyDataList.add(RowFactory.create(id, name, age));
        }

        // 더미 데이터의 컬럼 정의 (id, name, age)
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        });

        // Dataset 으로 변환
        Dataset<Row> dummyData = sparkSession.createDataFrame(dummyDataList, schema);

        // Iceberg 테이블에 더미 데이터 추가
        dummyData.write()
                .format("iceberg")
                .mode(SaveMode.Append)  // 데이터를 append 모드로 추가
                .save(ICEBERG_TABLE_PATH);

        log.info("Dummy data appended to Iceberg Table at {}", ICEBERG_TABLE_PATH);
    }

}
