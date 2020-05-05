package ru.kpfu.itis.transaction.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import ru.kpfu.itis.transaction.avro.Transaction;
import ru.kpfu.itis.transaction.domain.model.Topic;

import java.util.Collections;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
@PropertySource("classpath:application.properties")
public class TransactionConfiguration {

    @Value("${transaction-service.kafka.server}")
    private String bootstrapServer;

    @Value("${transaction-service.kafka.producer.clientId}")
    private String clientId;

    @Value("${transaction-service.schema-registry.server}")
    private String schemaRegistryServer;

    @Bean
    public KafkaProducer getTransactionsTopicProducer(final Topic<String, Transaction> topic) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        return new KafkaProducer<>(producerConfig,
                topic.keySerde().serializer(),
                topic.valueSerde().serializer());
    }

    @Bean("transactionsTopic")
    public Topic<String, Transaction> getTransactionsTopic() {
        SpecificAvroSerde<Transaction> transactionSpecificAvroSerde = new SpecificAvroSerde<>();
        transactionSpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer), false);
        return new Topic<>("transactions", Serdes.String(), transactionSpecificAvroSerde);
    }
}
