-- Initial schema migrations for REDDO
-- Creates the workflows and tasks execution status tracking tables

CREATE TABLE IF NOT EXISTS workflow_instances (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL, -- PENDING, RUNNING, COMPLETED, FAILED
    dag JSONB NOT NULL,          -- Full DAG structure with task dependencies
    payload JSONB,               -- Overall execution payload/variables
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS task_executions (
    id VARCHAR(64) PRIMARY KEY,
    workflow_instance_id VARCHAR(64) NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    task_key VARCHAR(255) NOT NULL, -- Unique key inside the DAG (e.g., task_a)
    status VARCHAR(50) NOT NULL,    -- PENDING, RUNNING, COMPLETED, FAILED, ROLLED_BACK
    input_data JSONB,
    output_data JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (workflow_instance_id, task_key)
);

CREATE INDEX IF NOT EXISTS idx_task_executions_wf_id ON task_executions(workflow_instance_id);
