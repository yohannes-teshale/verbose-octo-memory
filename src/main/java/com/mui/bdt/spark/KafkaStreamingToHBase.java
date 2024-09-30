package com.mui.bdt.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class KafkaStreamingToHBase implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String TABLE_NAME = "transactions";
    private static final String COLUMN_FAMILY = "cf";
    private static final String FRAUD_TOPIC = "fraudulent-transactions";
    private static KafkaProducer<String, String> producer;


    private transient SparkSession spark;
    private transient JavaStreamingContext streamingContext;
    private transient PipelineModel model;

    public static void main(String[] args) {
        KafkaStreamingToHBase app = new KafkaStreamingToHBase();
        app.start();
    }

    public void start() {
        try {
            SparkConf conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("Kafka Fraud Detection to HBase");

            spark = SparkSession.builder().config(conf).getOrCreate();
            streamingContext = new JavaStreamingContext(
                    JavaSparkContext.fromSparkContext(spark.sparkContext()),
                    Durations.seconds(10)
            );
            initializeKafkaProducer();


            model = PipelineModel.load("src/main/java/com/mui/bdt/spark/model/random_forest_model");

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", "localhost:9092");
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "fraud-group");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            String[] topics = {"topic-credit"};

            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(Arrays.asList(topics), kafkaParams));

            StructType schema = createSchema();

            stream.foreachRDD((JavaRDD<ConsumerRecord<String, String>> rdd) -> {
                if (!rdd.isEmpty()) {
                    try {
                        System.out.println("--- New RDD with " + rdd.count() + " records");

                        JavaRDD<Row> rowRDD = rdd.map(record -> {
                            String[] values = record.value().split(",");
                            if (values.length != 31) {
                                throw new IllegalArgumentException("Invalid transaction format. Expected 31 values, got " + values.length);
                            }
                            Object[] rowValues = Arrays.stream(values)
                                    .limit(30)
                                    .map(Double::parseDouble)
                                    .toArray();
                            return RowFactory.create(rowValues);
                        });

                        Dataset<Row> transactionData = spark.createDataFrame(rowRDD, schema);

                        Dataset<Row> predictions = model.transform(transactionData);

                        List<Row> results = predictions.select("prediction", "*").collectAsList();

                        try (Connection hbaseConnection = HBaseConnectionPool.getConnection()) {
                            createHBaseTableIfNotExists(hbaseConnection);
                            for (Row row : results) {
                                double prediction = row.getDouble(0);
                                String transaction = row.mkString(",");
                                if (prediction == 1.0) {
                                    System.out.println("Fraudulent transaction detected: " + transaction);
                                    writeToHBase(hbaseConnection, row, true);
                                    sendToKafka(transaction);

                                } else {
                                    System.out.println("Legitimate transaction: " + transaction);
                                    writeToHBase(hbaseConnection, row, false);
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing RDD: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            });

            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (Exception e) {
            System.err.println("An error occurred in the main method: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private static void initializeKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    private static void sendToKafka(String transaction) {
        ProducerRecord<String, String> record = new ProducerRecord<>(FRAUD_TOPIC, transaction);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending fraudulent transaction to Kafka: " + exception.getMessage());
            } else {
                System.out.println("Fraudulent transaction sent to Kafka topic: " + FRAUD_TOPIC);
            }
        });
    }

    private static StructType createSchema() {
        return new StructType()
                .add("V1", DataTypes.DoubleType)
                .add("V2", DataTypes.DoubleType)
                .add("V3", DataTypes.DoubleType)
                .add("V4", DataTypes.DoubleType)
                .add("V5", DataTypes.DoubleType)
                .add("V6", DataTypes.DoubleType)
                .add("V7", DataTypes.DoubleType)
                .add("V8", DataTypes.DoubleType)
                .add("V9", DataTypes.DoubleType)
                .add("V10", DataTypes.DoubleType)
                .add("V11", DataTypes.DoubleType)
                .add("V12", DataTypes.DoubleType)
                .add("V13", DataTypes.DoubleType)
                .add("V14", DataTypes.DoubleType)
                .add("V15", DataTypes.DoubleType)
                .add("V16", DataTypes.DoubleType)
                .add("V17", DataTypes.DoubleType)
                .add("V18", DataTypes.DoubleType)
                .add("V19", DataTypes.DoubleType)
                .add("V20", DataTypes.DoubleType)
                .add("V21", DataTypes.DoubleType)
                .add("V22", DataTypes.DoubleType)
                .add("V23", DataTypes.DoubleType)
                .add("V24", DataTypes.DoubleType)
                .add("V25", DataTypes.DoubleType)
                .add("V26", DataTypes.DoubleType)
                .add("V27", DataTypes.DoubleType)
                .add("V28", DataTypes.DoubleType)
                .add("Time", DataTypes.DoubleType)
                .add("Amount", DataTypes.DoubleType);
    }

    private static void createHBaseTableIfNotExists(Connection hbaseConnection) throws IOException {
        try (Admin admin = hbaseConnection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                tableDescriptorBuilder.setColumnFamily(cfDescriptorBuilder.build());
                admin.createTable(tableDescriptorBuilder.build());
                System.out.println("Table " + TABLE_NAME + " created successfully.");
            } else {
                System.out.println("Table " + TABLE_NAME + " already exists.");
            }
        }
    }

    private static void writeToHBase(Connection hbaseConnection, Row row, boolean isFraudulent) throws IOException {
        try (Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME))) {
            String rowKey = UUID.randomUUID().toString();
            Put put = new Put(Bytes.toBytes(rowKey));

            for (int i = 0; i < row.length(); i++) {
                String columnName = row.schema().fieldNames()[i];
                String value = row.get(i).toString();
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(columnName), Bytes.toBytes(value));
            }

            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("isFraudulent"), Bytes.toBytes(String.valueOf(isFraudulent)));

            table.put(put);
        }
    }
}

class HBaseConnectionPool {
    private static final Configuration config = HBaseConfiguration.create();
    private static Connection connection;

    static {
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
    }

    public static synchronized Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(config);
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}