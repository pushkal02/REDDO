package io.reddo.broker.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reddo.broker.dto.TaskMessage;
import io.reddo.broker.model.TaskExecution;
import io.reddo.broker.model.WorkflowInstance;
import io.reddo.broker.repository.TaskExecutionRepository;
import io.reddo.broker.repository.WorkflowInstanceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class SagaOrchestrator {

    private final RedissonClient redissonClient;
    private final WorkflowInstanceRepository workflowInstanceRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final SagaBusinessService businessService;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Entry point for processing a task message received from RabbitMQ.
     * Manages locking, idempotency check, business execution, and state transitions.
     */
    public void processTask(TaskMessage message) {
        String taskExecId = message.getTaskExecutionID();
        String lockKey = "lock:task-exec:" + taskExecId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // Acquire distributed lock with Watchdog enabled (wait up to 5s, lease time -1 turns on watchdog auto-renewal)
            log.info("[Orchestrator] Attempting to acquire lock for task execution {}", taskExecId);
            boolean locked = lock.tryLock(5, -1, TimeUnit.SECONDS);
            if (!locked) {
                log.warn("[Orchestrator] Failed to acquire lock for task {}. Duplicate message, discarding.", taskExecId);
                return;
            }

            log.info("[Orchestrator] Lock acquired for task {}. Checking execution state...", taskExecId);

            // Double check status inside database transaction (Idempotency check)
            TaskExecution taskExec = getTaskExecution(taskExecId);
            if (taskExec == null) {
                log.error("[Orchestrator] Task execution record {} not found in database.", taskExecId);
                return;
            }

            if ("COMPLETED".equals(taskExec.getStatus()) || "FAILED".equals(taskExec.getStatus()) || "ROLLED_BACK".equals(taskExec.getStatus())) {
                log.info("[Orchestrator] Task {} has already run. Current status: {}. Discarding message.", taskExecId, taskExec.getStatus());
                return;
            }

            // Mark task as RUNNING in database
            updateTaskStatus(taskExecId, "RUNNING", null, null);

            // Execute the business logic corresponding to the task key
            log.info("[Orchestrator] Executing business logic for task key: {}", taskExec.getTaskKey());
            executeBusinessTask(taskExec, message.getInputData());

            // On success: mark task as COMPLETED and check for downstream tasks
            log.info("[Orchestrator] Task {} executed successfully. Committing completion...", taskExecId);
            completeTaskAndProgress(taskExecId);

        } catch (Exception e) {
            log.error("[Orchestrator] Error occurred executing task {}: {}", taskExecId, e.getMessage());
            // On failure: rollback business state and log failure to metadata table in a separate transaction
            handleTaskFailure(message.getWorkflowInstanceID(), taskExecId, e.getMessage());
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
                log.info("[Orchestrator] Lock released for task {}", taskExecId);
            }
        }
    }

    @Transactional(readOnly = true)
    private TaskExecution getTaskExecution(String id) {
        return taskExecutionRepository.findById(id).orElse(null);
    }

    /**
     * Dispatches the task to the correct business logic method based on its taskKey.
     */
    private void executeBusinessTask(TaskExecution taskExec, JsonNode inputData) {
        String key = taskExec.getTaskKey();
        String workflowInstanceId = taskExec.getWorkflowInstance().getId();

        if (key.equals("create_order")) {
            // Inputs: order_id (use workflow instance id), account_id, amount
            String accountId = inputData.get("account_id").asText();
            BigDecimal amount = new BigDecimal(inputData.get("amount").asText());
            businessService.createOrder(workflowInstanceId, accountId, amount);

        } else if (key.equals("cancel_order")) {
            businessService.cancelOrder(workflowInstanceId);

        } else if (key.equals("charge_payment")) {
            String accountId = inputData.get("account_id").asText();
            BigDecimal amount = new BigDecimal(inputData.get("amount").asText());
            boolean fail = inputData.has("fail") && inputData.get("fail").asBoolean();
            businessService.chargePayment(accountId, amount, fail);

        } else if (key.equals("refund_payment")) {
            String accountId = inputData.get("account_id").asText();
            BigDecimal amount = new BigDecimal(inputData.get("amount").asText());
            businessService.refundPayment(accountId, amount);

        } else {
            log.info("[Orchestrator] Unknown task key {}. Treating as a mock/no-op step.", key);
        }
    }

    /**
     * Updates the status of a TaskExecution record.
     * Uses REQUIRES_NEW propagation to ensure changes are committed immediately,
     * preventing them from being rolled back if outer transaction fails.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateTaskStatus(String id, String status, String outputData, String errorMessage) {
        TaskExecution taskExec = taskExecutionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Task execution " + id + " not found"));
        
        taskExec.setStatus(status);
        if (outputData != null) {
            taskExec.setOutputData(outputData);
        }
        if (errorMessage != null) {
            taskExec.setErrorMessage(errorMessage);
        }
        taskExecutionRepository.save(taskExec);
        log.debug("[Orchestrator] Task execution {} status updated to {}", id, status);
    }

    /**
     * Marks the task as completed and evaluates the DAG to publish next tasks.
     * Runs in a standard transaction block.
     */
    @Transactional
    public void completeTaskAndProgress(String taskExecId) {
        // 1. Mark task as COMPLETED
        TaskExecution taskExec = taskExecutionRepository.findById(taskExecId)
                .orElseThrow(() -> new IllegalArgumentException("Task execution " + taskExecId + " not found"));
        taskExec.setStatus("COMPLETED");
        taskExec.setOutputData("{\"status\":\"success\"}");
        taskExecutionRepository.save(taskExec);

        WorkflowInstance instance = taskExec.getWorkflowInstance();
        String workflowId = instance.getId();

        // 2. Evaluate downstream tasks
        progressDAG(workflowId);
    }

    /**
     * Handles task failure: rolls back business, logs failure, and triggers Saga compensation.
     * Uses REQUIRES_NEW propagation so metadata logs persist.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void handleTaskFailure(String workflowId, String failedTaskExecId, String errorMessage) {
        log.warn("[Orchestrator] Handling failure of task {}. Transitioning workflow to COMPENSATING.", failedTaskExecId);

        // 1. Mark the failed task execution status as FAILED
        TaskExecution failedTask = taskExecutionRepository.findById(failedTaskExecId)
                .orElseThrow(() -> new IllegalArgumentException("Task execution " + failedTaskExecId + " not found"));
        failedTask.setStatus("FAILED");
        failedTask.setErrorMessage(errorMessage);
        taskExecutionRepository.save(failedTask);

        // 2. Transition Workflow Instance to COMPENSATING
        WorkflowInstance instance = workflowInstanceRepository.findById(workflowId)
                .orElseThrow(() -> new IllegalArgumentException("Workflow " + workflowId + " not found"));
        instance.setStatus("COMPENSATING");
        workflowInstanceRepository.save(instance);

        // 3. Trigger Compensation Steps for already COMPLETED tasks
        List<TaskExecution> executions = taskExecutionRepository.findByWorkflowInstance_Id(workflowId);
        
        try {
            JsonNode dagNode = objectMapper.readTree(instance.getDag());
            JsonNode tasksNode = dagNode.get("tasks");

            boolean compensationTriggered = false;

            for (TaskExecution exec : executions) {
                if ("COMPLETED".equals(exec.getStatus())) {
                    JsonNode taskDef = tasksNode.get(exec.getTaskKey());
                    if (taskDef != null && taskDef.has("compensation")) {
                        String compensationTaskKey = taskDef.get("compensation").asText();
                        log.info("[Orchestrator] Completed task {} has compensating action: {}", exec.getTaskKey(), compensationTaskKey);

                        // Find the compensating task execution record (pre-created with PENDING status)
                        Optional<TaskExecution> compExecOpt = taskExecutionRepository
                                .findByWorkflowInstance_IdAndTaskKey(workflowId, compensationTaskKey);

                        if (compExecOpt.isPresent()) {
                            TaskExecution compExec = compExecOpt.get();
                            compExec.setStatus("RUNNING");
                            taskExecutionRepository.save(compExec);

                            // Copy inputs from original task to compensating task (e.g. account_id, amount)
                            JsonNode originalInputs = taskDef.get("input_data");

                            // Publish compensating task message to RabbitMQ
                            publishTaskMessage(compExec.getId(), workflowId, compensationTaskKey, originalInputs, taskDef.get("worker").asText());
                            compensationTriggered = true;
                        } else {
                            log.error("[Orchestrator] Compensation task record {} not found in database.", compensationTaskKey);
                        }
                    }
                }
            }

            // If no compensations were defined or triggered, transition the workflow directly to FAILED
            if (!compensationTriggered) {
                log.info("[Orchestrator] No compensations defined for completed steps. Workflow fails immediately.");
                instance.setStatus("FAILED");
                workflowInstanceRepository.save(instance);
            }

        } catch (Exception e) {
            log.error("[Orchestrator] Failed to parse DAG or schedule compensations: {}", e.getMessage());
            instance.setStatus("FAILED");
            workflowInstanceRepository.save(instance);
        }
    }

    /**
     * Evaluates the DAG structure to see what needs to run next, or finishes the workflow.
     */
    @Transactional
    public void progressDAG(String workflowId) {
        WorkflowInstance instance = workflowInstanceRepository.findById(workflowId)
                .orElseThrow(() -> new IllegalArgumentException("Workflow instance not found: " + workflowId));

        List<TaskExecution> executions = taskExecutionRepository.findByWorkflowInstance_Id(workflowId);
        
        // Build map of taskKey -> TaskExecution for easy lookup
        Map<String, TaskExecution> execMap = new HashMap<>();
        for (TaskExecution exec : executions) {
            execMap.put(exec.getTaskKey(), exec);
        }

        try {
            JsonNode dagNode = objectMapper.readTree(instance.getDag());
            JsonNode tasksNode = dagNode.get("tasks");

            if ("COMPENSATING".equals(instance.getStatus())) {
                // If we are compensating, check if all active compensation tasks are finished.
                boolean compensationsActive = false;
                
                // Identify compensation tasks from DAG
                Set<String> compensationTaskKeys = new HashSet<>();
                Iterator<Map.Entry<String, JsonNode>> fields = tasksNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    if (entry.getValue().has("compensation")) {
                        compensationTaskKeys.add(entry.getValue().get("compensation").asText());
                    }
                }

                // Check statuses of compensating tasks
                for (String compKey : compensationTaskKeys) {
                    TaskExecution compExec = execMap.get(compKey);
                    if (compExec != null) {
                        if ("RUNNING".equals(compExec.getStatus()) || "PENDING".equals(compExec.getStatus())) {
                            // Compensation is still running
                            compensationsActive = true;
                            break;
                        }
                    }
                }

                if (!compensationsActive) {
                    log.info("[Orchestrator] All compensation tasks completed. Transitioning workflow status to FAILED.");
                    instance.setStatus("FAILED");
                    workflowInstanceRepository.save(instance);
                }
                return;
            }

            // Otherwise, we are in RUNNING mode. Check for PENDING tasks whose dependencies are met.
            boolean anyTaskRunning = false;
            boolean anyTaskPending = false;
            List<TaskExecution> tasksToTrigger = new ArrayList<>();
            List<String> workersToTrigger = new ArrayList<>();
            List<JsonNode> inputsToTrigger = new ArrayList<>();

            Iterator<Map.Entry<String, JsonNode>> fields = tasksNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String taskKey = entry.getKey();
                JsonNode taskDef = entry.getValue();

                TaskExecution exec = execMap.get(taskKey);
                if (exec == null) continue;

                if ("RUNNING".equals(exec.getStatus())) {
                    anyTaskRunning = true;
                } else if ("PENDING".equals(exec.getStatus())) {
                    anyTaskPending = true;

                    // Evaluate dependencies
                    JsonNode depsNode = taskDef.get("dependencies");
                    boolean dependenciesMet = true;

                    if (depsNode != null && depsNode.isArray()) {
                        for (JsonNode dep : depsNode) {
                            String depKey = dep.asText();
                            // Bypassing dummy dependency used to hide compensation tasks on startup
                            if ("__SAGA_FAIL__".equals(depKey)) {
                                dependenciesMet = false;
                                break;
                            }
                            TaskExecution depExec = execMap.get(depKey);
                            if (depExec == null || !"COMPLETED".equals(depExec.getStatus())) {
                                dependenciesMet = false;
                                break;
                            }
                        }
                    }

                    if (dependenciesMet) {
                        tasksToTrigger.add(exec);
                        workersToTrigger.add(taskDef.get("worker").asText());
                        inputsToTrigger.add(taskDef.get("input_data"));
                    }
                }
            }

            // Trigger ready tasks
            for (int i = 0; i < tasksToTrigger.size(); i++) {
                TaskExecution exec = tasksToTrigger.get(i);
                exec.setStatus("RUNNING");
                taskExecutionRepository.save(exec);

                publishTaskMessage(exec.getId(), workflowId, exec.getTaskKey(), inputsToTrigger.get(i), workersToTrigger.get(i));
                anyTaskRunning = true;
            }

            // If no tasks are running and none are pending, the entire workflow must be complete!
            if (!anyTaskRunning && !anyTaskPending) {
                log.info("[Orchestrator] All DAG tasks completed successfully! Transitioning workflow status to COMPLETED.");
                instance.setStatus("COMPLETED");
                workflowInstanceRepository.save(instance);
            }

        } catch (Exception e) {
            log.error("[Orchestrator] Error processing DAG state: {}", e.getMessage());
            instance.setStatus("FAILED");
            workflowInstanceRepository.save(instance);
        }
    }

    /**
     * Publishes a task message to the appropriate worker queue in RabbitMQ.
     */
    private void publishTaskMessage(String taskExecId, String workflowId, String taskKey, JsonNode inputData, String worker) {
        String routingKey;
        if ("java".equalsIgnoreCase(worker)) {
            routingKey = "tasks.java";
        } else if ("node".equalsIgnoreCase(worker)) {
            routingKey = "tasks.node";
        } else {
            log.error("[Orchestrator] Unsupported worker type {} for task {}.", worker, taskKey);
            return;
        }

        TaskMessage msg = TaskMessage.builder()
                .taskExecutionID(taskExecId)
                .workflowInstanceID(workflowId)
                .taskKey(taskKey)
                .inputData(inputData)
                .build();

        log.info("[Orchestrator] Publishing task {} ({}) to routing key {}", taskKey, taskExecId, routingKey);
        rabbitTemplate.convertAndSend("workflow-exchange", routingKey, msg);
    }
}
