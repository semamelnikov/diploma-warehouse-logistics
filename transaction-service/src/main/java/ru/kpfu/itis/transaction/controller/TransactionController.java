package ru.kpfu.itis.transaction.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.kpfu.itis.transaction.domain.model.dto.TransactionDto;
import ru.kpfu.itis.transaction.service.TransactionService;

@RestController
@RequestMapping("/transactions")
public class TransactionController {
    private final TransactionService transactionService;

    public TransactionController(final TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public void submitTransaction(@RequestBody TransactionDto transactionDto) {
        transactionService.submitTransaction(TransactionDto.toTransaction(transactionDto));
    }
}
