package io.reddo.broker.service;

import io.reddo.broker.model.SagaAccount;
import io.reddo.broker.model.SagaOrder;
import io.reddo.broker.repository.SagaAccountRepository;
import io.reddo.broker.repository.SagaOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
@Slf4j
public class SagaBusinessService {

    private final SagaAccountRepository accountRepository;
    private final SagaOrderRepository orderRepository;

    /**
     * Executes the simulated "create_order" step.
     * Records the order in the database with PENDING status.
     */
    @Transactional
    public void createOrder(String orderId, String accountId, BigDecimal amount) {
        log.info("[Business] Creating order {} for account {} with amount {}", orderId, accountId, amount);
        
        // Ensure account exists
        SagaAccount account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account " + accountId + " not found"));

        SagaOrder order = SagaOrder.builder()
                .orderId(orderId)
                .accountId(account.getAccountId())
                .amount(amount)
                .status("PENDING")
                .build();

        orderRepository.save(order);
        log.info("[Business] Order {} saved successfully in PENDING state.", orderId);
    }

    /**
     * Executes the simulated "cancel_order" compensating step.
     * Reverts order status from PENDING/COMPLETED to CANCELLED.
     */
    @Transactional
    public void cancelOrder(String orderId) {
        log.info("[Business] Saga compensation: Cancelling order {}", orderId);
        
        SagaOrder order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order " + orderId + " not found"));

        order.setStatus("CANCELLED");
        orderRepository.save(order);
        log.info("[Business] Order {} status reverted to CANCELLED.", orderId);
    }

    /**
     * Executes the simulated "charge_payment" step.
     * Deducts the amount from the account balance.
     * If the 'fail' flag is set, it throws a RuntimeException to trigger transaction rollback.
     */
    @Transactional
    public void chargePayment(String accountId, BigDecimal amount, boolean fail) {
        log.info("[Business] Charging payment of {} from account {}", amount, accountId);
        
        SagaAccount account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account " + accountId + " not found"));

        if (account.getBalance().compareTo(amount) < 0) {
            throw new IllegalStateException("Insufficient funds: Account has " + account.getBalance() + ", requires " + amount);
        }

        BigDecimal newBalance = account.getBalance().subtract(amount);
        account.setBalance(newBalance);
        accountRepository.save(account);
        log.info("[Business] Balance updated successfully. New balance for {} is {}", accountId, newBalance);

        // Deterministic failure injection to test Saga Rollback
        if (fail) {
            log.warn("[Business] FORCED ERROR: Simulating payment gateway crash/timeout. Triggering transactional rollback!");
            throw new RuntimeException("Payment Gateway Timeout (Forced Saga Rollback)");
        }
    }

    /**
     * Executes the simulated "refund_payment" compensating step.
     * Adds the deducted amount back to the account.
     */
    @Transactional
    public void refundPayment(String accountId, BigDecimal amount) {
        log.info("[Business] Saga compensation: Refunding {} to account {}", amount, accountId);
        
        SagaAccount account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account " + accountId + " not found"));

        BigDecimal newBalance = account.getBalance().add(amount);
        account.setBalance(newBalance);
        accountRepository.save(account);
        log.info("[Business] Refund completed. New balance for {} is {}", accountId, newBalance);
    }
}
