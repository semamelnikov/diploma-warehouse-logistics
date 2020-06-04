package ru.kpfu.itis.delivery.service;

import ru.kpfu.itis.batch.avro.DeliveryBatch;

public interface TruckService {
    void createDelivery(final DeliveryBatch deliveryBatch);
}
