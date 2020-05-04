package ru.kpfu.itis.transaction.domain.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import ru.kpfu.itis.transaction.avro.Transaction;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class TransactionDto {
    public String id;
    public long shopId;
    public long productId;
    public int quantity;
    public double price;

    public static Transaction toTransaction(final TransactionDto dto) {
        return Transaction.newBuilder()
                .setId(dto.getId())
                .setShopId(dto.getShopId())
                .setProductId(dto.getProductId())
                .setQuantity(dto.getQuantity())
                .setPrice(dto.getPrice())
                .build();
    }
}
