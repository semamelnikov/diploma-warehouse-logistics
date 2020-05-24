package ru.kpfu.itis.delivery.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class ApplicationMetrics {
    private final Counter deliveryCounter;
    private final Counter deliveryBatchCounter;

    public ApplicationMetrics(final MeterRegistry meterRegistry) {
        deliveryCounter = Counter.builder("delivery.counter").register(meterRegistry);
        deliveryBatchCounter = Counter.builder("delivery.batch.counter").register(meterRegistry);
    }

    public Counter getDeliveryCounter() {
        return deliveryCounter;
    }

    public Counter getDeliveryBatchCounter() {
        return deliveryBatchCounter;
    }
}
