package ru.kpfu.itis.transaction.service.impl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.kpfu.itis.transaction.avro.Transaction;
import ru.kpfu.itis.transaction.domain.model.Topic;
import ru.kpfu.itis.transaction.service.TransactionService;

@Service
public class TransactionServiceImpl implements TransactionService {

    private final Topic<String, Transaction> transactionsTopic;
    private final KafkaProducer<String, Transaction> producer;

    public TransactionServiceImpl(final Topic<String, Transaction> transactionsTopic,
                                  final KafkaProducer<String, Transaction> producer) {
        this.transactionsTopic = transactionsTopic;
        this.producer = producer;
    }

    @Override
    public void submitTransaction(final Transaction transaction) {
        producer.send(
                new ProducerRecord<>(transactionsTopic.name(), transaction.getId(), transaction)
        );
    }
}
