package io.reddo.broker.repository;

import io.reddo.broker.model.SagaOrder;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SagaOrderRepository extends JpaRepository<SagaOrder, String> {
}
