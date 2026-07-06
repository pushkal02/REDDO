package io.reddo.broker.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskMessage {

    @JsonProperty("task_execution_id")
    private String taskExecutionID;

    @JsonProperty("workflow_instance_id")
    private String workflowInstanceID;

    @JsonProperty("task_key")
    private String taskKey;

    @JsonProperty("input_data")
    private JsonNode inputData; // Represents raw JSON payload of the task

    @JsonProperty("correlation_id")
    private String correlationID;

    @JsonProperty("request_id")
    private String requestID;
}
