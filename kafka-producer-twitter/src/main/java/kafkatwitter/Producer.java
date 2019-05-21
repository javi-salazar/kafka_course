package kafkatwitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private String bootstrapServers = "127.0.0.1:9092";
    private Properties properties;
    private KafkaProducer<String, String> kafkaProducer;
    private ProducerRecord<String, String> record;

    private Logger logger;

    public Producer() {
        this.logger = LoggerFactory.getLogger(Producer.class.getName());
        this.properties = new Properties();
        this.setProducerProperties();
        this.createProducer();
    }

    private void setProducerProperties() {
        this.properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                this.bootstrapServers);
        this.properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        this.properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // safe producer properties
        this.properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                "true");
        this.properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        this.properties.setProperty(ProducerConfig.RETRIES_CONFIG,
                Integer.toString(Integer.MAX_VALUE));
        this.properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                "5");

        // high throughput settings (at the expense of a bit of latency and
        // CPU usage)
        this.properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                "snappy");
        this.properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        this.properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,
                Integer.toString(32 * 1024));
    }

    private void createProducer() {
        this.kafkaProducer = new KafkaProducer<>(this.properties);
    }

    public void setRecord(String topic, String key,
                                        String value) {
        this.record = new ProducerRecord<>(topic, key, value);
    }

    public void sendRecord() {
        this.kafkaProducer.send(this.record, (recordMetadata, e) -> {
           if (e == null) {
               logger.info("Received new metadata: \n" +
                       "Topic: " + recordMetadata.topic() + "\n" +
                       "Partition: " + recordMetadata.partition() + "\n" +
                       "Offset: " + recordMetadata.offset() + "\n" +
                       "Timestamp: " + recordMetadata.timestamp());
           } else {
               logger.error("Error while producing", e);
           }
        });
    }

    public void commit() {
        this.kafkaProducer.flush();
    }

    public void end() {
        this.kafkaProducer.close();
    }
}
