package ru.kpfu.itis.delivery.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.delivery.domain.model.Topic;

import java.util.Properties;

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
                                     final DeliveryProcessor deliveryProcessor
    ) {
        final StreamsBuilder builder = new StreamsBuilder();
        final StoreBuilder<KeyValueStore<Long, DeliveryBatch>> deliveryBatchStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(DeliveryProcessor.DELIVERY_BATCH_STORE_NAME),
                        Serdes.Long(),
                        deliveryBatchTopic.valueSerde()
                );
        builder.addStateStore(deliveryBatchStoreBuilder);

        KStream<String, Delivery> deliveries = builder.stream(
                deliveryTopic.name(), Consumed.with(deliveryTopic.keySerde(), deliveryTopic.valueSerde()));
        deliveries.process(() -> deliveryProcessor, DeliveryProcessor.DELIVERY_BATCH_STORE_NAME);

        return builder.build();
    }
}
