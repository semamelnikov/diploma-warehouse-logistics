package ru.kpfu.itis.transaction.service;

import ru.kpfu.itis.transaction.avro.Transaction;

public interface TransactionService {
    void submitTransaction(Transaction transaction);
}
