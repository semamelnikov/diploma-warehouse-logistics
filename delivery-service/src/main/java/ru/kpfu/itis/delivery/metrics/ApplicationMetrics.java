package ru.kpfu.itis.delivery.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class ApplicationMetrics {
    private static final MeterRegistry METER_REGISTRY = new SimpleMeterRegistry();
    private final Counter deliveryCounter = Counter.builder("delivery.counter").register(METER_REGISTRY);
    private final Counter deliveryBatchCounter = Counter.builder("delivery.batch.counter").register(METER_REGISTRY);

    public Counter getDeliveryCounter() {
        return deliveryCounter;
    }

    public Counter getDeliveryBatchCounter() {
        return deliveryBatchCounter;
    }
}
