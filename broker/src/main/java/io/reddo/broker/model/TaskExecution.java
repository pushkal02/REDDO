package io.reddo.broker.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;

@Entity
@Table(name = "task_executions", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"workflow_instance_id", "task_key"})
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TaskExecution {

    @Id
    @Column(length = 64)
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_instance_id", nullable = false)
    private WorkflowInstance workflowInstance;

    @Column(name = "task_key", nullable = false)
    private String taskKey; // e.g., task_a, charge_payment

    @Column(nullable = false, length = 50)
    private String status; // PENDING, RUNNING, COMPLETED, FAILED, ROLLED_BACK

    @Column(name = "input_data", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String inputData;

    @Column(name = "output_data", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String outputData;

    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", insertable = false, updatable = false)
    private OffsetDateTime updatedAt;
}
