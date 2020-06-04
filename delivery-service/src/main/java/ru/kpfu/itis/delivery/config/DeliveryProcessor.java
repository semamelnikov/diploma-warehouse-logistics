package ru.kpfu.itis.delivery.config;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.batch.avro.ProductQuantity;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.delivery.service.TruckService;

import java.time.Duration;
import java.util.UUID;

@Component
public class DeliveryProcessor extends AbstractProcessor<String, Delivery> {
    public static final String DELIVERY_BATCH_STORE_NAME = "delivery-batch-store-" + UUID.randomUUID().toString();

    private final TruckService truckService;
    private ProcessorContext context;
    private KeyValueStore<Long, DeliveryBatch> deliveryBatchStore;

    @Autowired
    public DeliveryProcessor(final TruckService truckService) {
        this.truckService = truckService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
        this.context = context;

        this.deliveryBatchStore = (KeyValueStore) context.getStateStore(DELIVERY_BATCH_STORE_NAME);

        this.context.schedule(Duration.ofMinutes(5), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            final KeyValueIterator<Long, DeliveryBatch> storeIterator = this.deliveryBatchStore.all();
            while (storeIterator.hasNext()) {
                final KeyValue<Long, DeliveryBatch> deliveryBatchKeyValue = storeIterator.next();
                final DeliveryBatch deliveryBatch = deliveryBatchKeyValue.value;
                truckService.createDelivery(deliveryBatch);
                this.deliveryBatchStore.delete(deliveryBatch.getShopId());
            }
            storeIterator.close();
        });
    }

    @Override
    public void process(String s, Delivery delivery) {
        final ProductQuantity productQuantity = ProductQuantity.newBuilder()
                .setProductId(delivery.getProductId())
                .setQuantity(delivery.getQuantity())
                .build();
        final DeliveryBatch currentDeliveryBatch = deliveryBatchStore.get(delivery.getShopId());
        if (ObjectUtils.isNotEmpty(currentDeliveryBatch)) {
            currentDeliveryBatch.getProductQuantities().add(productQuantity);
            deliveryBatchStore.put(currentDeliveryBatch.getShopId(), currentDeliveryBatch);
        } else {
            final DeliveryBatch deliveryBatch = DeliveryBatch.newBuilder()
                    .setId(UUID.randomUUID().toString())
                    .setShopId(delivery.getShopId())
                    .build();
            deliveryBatch.getProductQuantities().add(productQuantity);
            deliveryBatchStore.put(delivery.getShopId(), deliveryBatch);
        }
    }

    @Override
    public void close() {
        super.close();
    }
}
