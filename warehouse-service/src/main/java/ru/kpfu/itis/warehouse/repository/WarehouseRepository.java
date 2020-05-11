package ru.kpfu.itis.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.kpfu.itis.warehouse.domain.model.entity.WarehouseRecord;

public interface WarehouseRepository extends JpaRepository<WarehouseRecord, Long> {
    WarehouseRecord findByShopIdAndProductId(Long shopId, Long productId);
}
