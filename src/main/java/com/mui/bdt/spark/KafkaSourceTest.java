package com.mui.bdt.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("Kafka Source Test")
                .master("local[*]")
                .getOrCreate();

        StreamingQuery query = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test-topic")
                .load()
                .writeStream()
                .format("console")
                .start();

        query.awaitTermination(10000);
        query.stop();
        spark.stop();
    }
}