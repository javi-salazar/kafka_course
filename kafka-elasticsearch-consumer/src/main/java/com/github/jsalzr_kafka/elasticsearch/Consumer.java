package com.github.jsalzr_kafka.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private RestHighLevelClient client;
    private String bootstrapServers = "127.0.0.1:9092";
    private String groupId = "kafka-demo-elasticsearch";
    private Properties properties;
    private KafkaConsumer<String, String> kafkaConsumer;
    private ConsumerRecords<String, String> records;
    private String topic;
    private JsonParser jsonParser;

    private Logger logger;

    public Consumer(RestHighLevelClient client, String topic) {
        this.topic = topic;
        this.client = client;
        this.logger = LoggerFactory.getLogger(Consumer.class.getName());
        this.properties = new Properties();
        this.jsonParser = new JsonParser();
        this.setConsumerProperties();
        this.createConsumer();
        this.subscribeConsumer();
    }

    private void setConsumerProperties() {
        this.properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        this.properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    }

    private void createConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(properties);
    }

    private void subscribeConsumer() {
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    private String extractIdFromTweet(String tweetJson) {
        return this.jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public void startConsuming() throws IOException {
        this.records = this.kafkaConsumer.poll(Duration.ofMillis(100));

        BulkRequest bulkRequest = new BulkRequest();

        Integer recordCount = records.count();

        logger.info("Received: " + records.count() + " records");
        for (ConsumerRecord<String, String> record : this.records) {
            // Using the IDs makes our request idempotent
            // 2 strategies for creating ids
            // kafka generic id
            // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

            // twitter feed specific id
            try {
                String id = this.extractIdFromTweet(record.value());

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest); // add to bulk request (takes no time)
            } catch (NullPointerException e) {
                logger.warn("Skipping bad data: " + record.value());
            }

        }

        if (recordCount > 0) {
            BulkResponse bulkResponse = this.client.bulk(bulkRequest, RequestOptions.DEFAULT);

            logger.info("Committing offsets...");
            kafkaConsumer.commitSync();
            logger.info("Offsets have been committed");

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
