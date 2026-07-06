package io.reddo.broker.repository;

import io.reddo.broker.model.SagaAccount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SagaAccountRepository extends JpaRepository<SagaAccount, String> {
}
