package tech.marcusvieira.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Properties;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import tech.marcusvieira.schema.Client;

public class KafkaAvroProducer {

    private final static String TOPIC = "new-clients";

    private static Producer<Long, Client> createAvroProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-producer-avro");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 2);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new KafkaProducer<>(props);
    }

    public static void main(String... args) {

        Producer<Long, Client> producer = createAvroProducer();

        Client client = Client.newBuilder()
            .setClientId("clientId")
            .setFirstName("Paul")
            .setLastName("Hermman")
            .setAge(55)
            .build();

        IntStream.range(1, 100).forEach(index -> {
            producer
                .send(new ProducerRecord<>(TOPIC, 1L * index, client),
                    (metadata, exception) -> {
                        System.out.printf("Sending message key %d topic %s partition %s offset %s exception %s \n",
                            1999L * index, metadata.topic(), metadata.partition(), metadata.offset(),
                            exception.toString());
                    });
        });
        producer.flush();
        producer.close();
    }
}
