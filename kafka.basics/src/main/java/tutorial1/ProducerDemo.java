package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
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

        // Create producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<>("first_topic", "hello world");

        // Send data - async
        producer.send(record);

        // wait for data to flush it (actually send it)
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
