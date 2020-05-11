package ru.kpfu.itis.warehouse.domain.model.entity;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "warehouse")
@Data
public class WarehouseRecord {
    @Id
    private Long id;
    private Long shopId;
    private Long productId;
    private Integer inStock;
    private Integer deliveryThreshold;
}
