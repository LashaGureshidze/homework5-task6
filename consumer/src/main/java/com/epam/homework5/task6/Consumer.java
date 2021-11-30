package com.epam.homework5.task6;

import com.epam.homework5.task6.db.ConnectionManager;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static org.apache.kafka.clients.consumer.Consumer<String, GenericRecord> consumer;
    private static final String TOPIC = "EPAM-Secret-Messages";

    public static void main(String[] args) {
        initDbSchema();
        initKafkaConsumer();

        try {
            while (true) {
                //wait 500 millis
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //read messages
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());

                    saveDataInDb(record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void saveDataInDb(GenericRecord value) {
        String insertSQL = "INSERT INTO messages(message, message_time) values(?, ?)";

        Connection connection = ConnectionManager.getInstance().getConnection();
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            pstmt.setString(1, value.get("message").toString());
            pstmt.setLong(2, (long)value.get("timestamp"));
            pstmt.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void initDbSchema() {
        String tableSql = "CREATE TABLE IF NOT EXISTS messages"
                + "(id serial PRIMARY KEY, "
                + "message varchar(200),"
                + "message_time bigint)";

        Connection connection = ConnectionManager.getInstance().getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(tableSql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    private static void initKafkaConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));
    }
}

