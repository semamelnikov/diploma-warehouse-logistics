package ru.kpfu.itis.warehouse.config;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.transaction.avro.Transaction;
import ru.kpfu.itis.warehouse.domain.model.Topic;
import ru.kpfu.itis.warehouse.domain.model.entity.WarehouseRecord;
import ru.kpfu.itis.warehouse.repository.WarehouseRepository;

import java.util.Properties;
import java.util.UUID;

@Configuration
public class WarehouseStreamsConfiguration {

    @Bean
    public KafkaStreams warehouseKafkaStreams(final Topology warehouseTopology,
                                              @Qualifier("brokerProperties") final Properties props) {
        final KafkaStreams warehouseKafkaStreams = new KafkaStreams(warehouseTopology, props);
        warehouseKafkaStreams.start();
        return warehouseKafkaStreams;
    }

    @Bean
    public Topology warehouseTopology(@Qualifier("transactionsTopic") final Topic<String, Transaction> transactionsTopic,
                                      @Qualifier("deliveryTopic") final Topic<String, Delivery> deliveryTopic,
                                      final WarehouseRepository warehouseRepository) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Transaction> transactions = builder.stream(
                transactionsTopic.name(), Consumed.with(transactionsTopic.keySerde(), transactionsTopic.valueSerde()));
        transactions
                .map((id, transaction) -> {
                    final WarehouseRecord warehouseRecord = warehouseRepository.findByShopIdAndProductId(
                            transaction.getShopId(), transaction.getProductId()
                    );
                    final int updatedInStock = warehouseRecord.getInStock() - transaction.getQuantity();
                    final Delivery.Builder deliveryBuilder = Delivery.newBuilder();
                    if (updatedInStock < warehouseRecord.getDeliveryThreshold()) {
                        final int inStockAfterDelivery = 2 * warehouseRecord.getDeliveryThreshold();
                        deliveryBuilder.setId(UUID.randomUUID().toString())
                                .setShopId(transaction.getShopId())
                                .setProductId(transaction.getProductId())
                                .setQuantity(inStockAfterDelivery - updatedInStock);
                        warehouseRecord.setInStock(inStockAfterDelivery);
                    } else {
                        warehouseRecord.setInStock(updatedInStock);
                    }
                    warehouseRepository.save(warehouseRecord);
                    if (StringUtils.isNotBlank(deliveryBuilder.getId())) {
                        final Delivery delivery = deliveryBuilder.build();
                        return KeyValue.pair(delivery.getId(), delivery);
                    }
                    return KeyValue.pair(id, null);
                })
                .filter((id, delivery) -> ObjectUtils.isNotEmpty(delivery))
                .to(deliveryTopic.name(), Produced.with(deliveryTopic.keySerde(), deliveryTopic.valueSerde()));
        return builder.build();
    }
}
