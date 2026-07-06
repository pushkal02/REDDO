package io.reddo.broker.repository;

import io.reddo.broker.model.WorkflowInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WorkflowInstanceRepository extends JpaRepository<WorkflowInstance, String> {
}
