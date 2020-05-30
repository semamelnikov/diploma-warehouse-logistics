package ru.kpfu.itis.delivery.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.batch.avro.ProductQuantity;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.delivery.domain.model.Topic;
import ru.kpfu.itis.delivery.metrics.ApplicationMetrics;

import java.time.Duration;
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
                                     @Qualifier("deliveryBatchTopic") final Topic<String, DeliveryBatch> deliveryBatchTopic,
                                     final ApplicationMetrics applicationMetrics
    ) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Delivery> deliveries = builder.stream(
                deliveryTopic.name(), Consumed.with(deliveryTopic.keySerde(), deliveryTopic.valueSerde()));

        deliveries
                .peek((key, value) -> applicationMetrics.getDeliveryCounter().increment())
                .groupBy((key, delivery) -> delivery.getShopId().toString(), Grouped.with(Serdes.String(), deliveryTopic.valueSerde()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).grace(Duration.ZERO))
                .aggregate(
                        DeliveryBatch.newBuilder()::build,
                        (aggKey, newValue, aggValue) -> {
                            aggValue.setId(aggKey);
                            aggValue.setShopId(Long.parseLong(aggKey));
                            List<ProductQuantity> productQuantities = aggValue.getProductQuantities();
                            productQuantities.add(
                                    ProductQuantity.newBuilder()
                                            .setProductId(newValue.getProductId())
                                            .setQuantity(newValue.getQuantity())
                                            .build()
                            );
                            return aggValue;
                        },
                        Materialized.<String, DeliveryBatch, WindowStore<Bytes, byte[]>>as("windowed-delivery-batch-store-" + UUID.randomUUID().toString())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(deliveryBatchTopic.valueSerde())
                                .withRetention(Duration.ofMinutes(5))
                )
                .toStream()
                .peek((key, value) -> applicationMetrics.getDeliveryBatchCounter().increment())
                .to(deliveryBatchTopic.name(), Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), deliveryBatchTopic.valueSerde()));

        return builder.build();
    }
}
