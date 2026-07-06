package io.reddo.broker.repository;

import io.reddo.broker.model.TaskExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface TaskExecutionRepository extends JpaRepository<TaskExecution, String> {
    List<TaskExecution> findByWorkflowInstance_Id(String workflowInstanceId);
    
    Optional<TaskExecution> findByWorkflowInstance_IdAndTaskKey(String workflowInstanceId, String taskKey);
}
