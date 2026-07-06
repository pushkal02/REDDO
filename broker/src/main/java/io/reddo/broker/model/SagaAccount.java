package io.reddo.broker.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;
import java.math.BigDecimal;

@Entity
@Table(name = "saga_accounts")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SagaAccount {

    @Id
    @Column(name = "account_id", length = 64)
    private String accountId;

    @Column(nullable = false, precision = 15, scale = 2)
    private BigDecimal balance;
}
