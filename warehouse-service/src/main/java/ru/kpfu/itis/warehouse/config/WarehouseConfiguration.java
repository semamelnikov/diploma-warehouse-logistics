package ru.kpfu.itis.warehouse.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import ru.kpfu.itis.transaction.avro.Delivery;
import ru.kpfu.itis.transaction.avro.Transaction;
import ru.kpfu.itis.warehouse.domain.model.Topic;

import java.util.Collections;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
@PropertySource("classpath:application.properties")
public class WarehouseConfiguration {
    @Value("${warehouse-service.kafka.application-id}")
    private String applicationId;

    @Value("${warehouse-service.kafka.server}")
    private String bootstrapServer;

    @Value("${warehouse-service.schema-registry.server}")
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

    @Bean("transactionsTopic")
    public Topic<String, Transaction> getTransactionsTopic() {
        SpecificAvroSerde<Transaction> transactionSpecificAvroSerde = new SpecificAvroSerde<>();
        transactionSpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer), false);
        return new Topic<>("transactions", Serdes.String(), transactionSpecificAvroSerde);
    }

    @Bean("deliveryTopic")
    public Topic<String, Delivery> getDeliveryTopic() {
        SpecificAvroSerde<Delivery> deliverySpecificAvroSerde = new SpecificAvroSerde<>();
        deliverySpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer), false);
        return new Topic<>("delivery", Serdes.String(), deliverySpecificAvroSerde);
    }
}
