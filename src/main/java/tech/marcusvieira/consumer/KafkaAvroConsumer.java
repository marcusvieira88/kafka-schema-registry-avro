package tech.marcusvieira.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import tech.marcusvieira.schema.Client;

public class KafkaAvroConsumer {

    private final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private final static String TOPIC = "new-clients";

    private static Consumer<Long, Client> createAvroConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-avro");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
    }

    public static void main(String... args) {
        final Consumer<Long, Client> consumer = createAvroConsumer();

        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            final ConsumerRecords<Long, Client> records = consumer.poll(100);
            records.forEach(record -> {
                Client client = record.value();
                System.out.printf("topic %s partition %d offset %d client %s \n", record.topic(), record.partition(),
                    record.offset(), client);
            });
        }
    }
}
