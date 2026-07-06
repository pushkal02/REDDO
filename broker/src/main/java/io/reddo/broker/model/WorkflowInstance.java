package io.reddo.broker.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;

@Entity
@Table(name = "workflow_instances")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WorkflowInstance {

    @Id
    @Column(length = 64)
    private String id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, length = 50)
    private String status; // PENDING, RUNNING, COMPLETED, FAILED, COMPENSATING

    @Column(nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String dag; // Full DAG structure with task definitions

    @Column(columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload; // Overall execution payload/variables

    @Column(name = "created_at", insertable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", insertable = false, updatable = false)
    private OffsetDateTime updatedAt;
}
