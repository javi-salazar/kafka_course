package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeysDemo {

    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerKeysDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        // Steps to create a producer
        // Create producer properties
        Properties properties = new Properties();

        // Old way of setting properties by hardcoding the property names
        //properties.setProperty("bootstrap.server", bootstrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());

        // New way
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        // Key and value serializer help kafka know what type of value we are sending
        // to it and how should that be serialized. Because Kafka will convert whatever
        // we sent to bytes. In this case we are passing strings so we pass the
        // String serializer.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello from java " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // Create producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key); // log the key

            // Send data - async
            producer.send(record, (recordMetadata, e) -> {
                // executes every time a record is successfully sent or an
                // exception is thrown
                if(e == null) {
                    // the record was sent
                    logger.info("Received new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // block send() to make it synchronous -- do not do in
            // prod!
        }
        // wait for data to flush it (actually send it)
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
