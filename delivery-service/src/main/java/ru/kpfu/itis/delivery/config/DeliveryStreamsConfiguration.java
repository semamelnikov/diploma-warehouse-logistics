package ru.kpfu.itis.delivery.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.batch.avro.ProductQuantity;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.delivery.domain.model.Topic;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Configuration
public class DeliveryStreamsConfiguration {

    @Bean
    public KafkaStreams deliveryKafkaStreams(final Topology deliveryTopology,
                                             @Qualifier("brokerProperties") final Properties props) {
        final KafkaStreams deliveryKafkaStreams = new KafkaStreams(deliveryTopology, props);
        deliveryKafkaStreams.start();
        return deliveryKafkaStreams;
    }

    @Bean
    public Topology deliveryTopology(@Qualifier("deliveryTopic") final Topic<String, Delivery> deliveryTopic,
                                     @Qualifier("deliveryBatchTopic") final Topic<Long, DeliveryBatch> deliveryBatchTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Delivery> deliveries = builder.stream(
                deliveryTopic.name(), Consumed.with(deliveryTopic.keySerde(), deliveryTopic.valueSerde()));

        deliveries
                .groupBy((key, delivery) -> delivery.getShopId(), Grouped.with(Serdes.Long(), deliveryTopic.valueSerde()))
                .aggregate(
                        DeliveryBatch.newBuilder()::build,
                        (aggKey, newValue, aggValue) -> {
                            aggValue.setId(aggKey.toString());
                            aggValue.setShopId(aggKey);
                            List<ProductQuantity> productQuantities = aggValue.getProductQuantities();
                            productQuantities.add(
                                    ProductQuantity.newBuilder()
                                            .setProductId(newValue.getProductId())
                                            .setQuantity(newValue.getQuantity())
                                            .build()
                            );
                            return aggValue;
                        },
                        Materialized.<Long, DeliveryBatch, KeyValueStore<Bytes, byte[]>>as("delivery-batch-store-" + UUID.randomUUID().toString())
                                .withKeySerde(Serdes.Long()).withValueSerde(deliveryBatchTopic.valueSerde())
                )
                .toStream()
                .to(deliveryBatchTopic.name(), Produced.with(deliveryBatchTopic.keySerde(), deliveryBatchTopic.valueSerde()));

        return builder.build();
    }
}
