package ru.kpfu.itis.delivery.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.delivery.domain.model.Topic;

import java.util.Collections;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
@PropertySource("classpath:application.properties")
public class DeliveryConfiguration {
    @Value("${delivery-service.kafka.application-id}")
    private String applicationId;

    @Value("${delivery-service.kafka.server}")
    private String bootstrapServer;

    @Value("${delivery-service.schema-registry.server}")
    private String schemaRegistryServer;

    @Bean("brokerProperties")
    public Properties getBrokerProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    @Bean("deliveryTopic")
    public Topic<String, Delivery> getDeliveryTopic() {
        SpecificAvroSerde<Delivery> deliverySpecificAvroSerde = new SpecificAvroSerde<>();
        deliverySpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer), false);
        return new Topic<>("delivery", Serdes.String(), deliverySpecificAvroSerde);
    }

    @Bean("deliveryBatchTopic")
    public Topic<Long, DeliveryBatch> getDeliveryBatchTopic() {
        SpecificAvroSerde<DeliveryBatch> deliveryBatchSpecificAvroSerde = new SpecificAvroSerde<>();
        deliveryBatchSpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer), false);
        return new Topic<>("delivery-batch", Serdes.Long(), deliveryBatchSpecificAvroSerde);
    }
}
