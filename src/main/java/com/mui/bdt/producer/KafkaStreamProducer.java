package com.mui.bdt.producer;

import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

public class KafkaStreamProducer {

    private static final String TOPIC = "topic-credit";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try (CSVReader reader = new CSVReader(
                new InputStreamReader(Objects.requireNonNull(KafkaStreamProducer.class.getClassLoader().getResourceAsStream("input.csv"))))) {

            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                String csvLine = String.join(",", nextLine);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, csvLine);
                producer.send(record);
                Thread.sleep(1000);
                System.out.println("Sent: " + csvLine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}