package ru.kpfu.itis.delivery.service.impl;

import org.springframework.stereotype.Service;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.delivery.service.TruckService;

@Service
public class TruckServiceImpl implements TruckService {
    @Override
    public void createDelivery(final DeliveryBatch deliveryBatch) {
        System.out.println("*: TruckServiceImpl deliveryBatch: " + deliveryBatch.toString());
    }
}
