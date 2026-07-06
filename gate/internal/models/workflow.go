package models

import "encoding/json"

// WorkflowRequest represents the incoming HTTP payload for a workflow submission.
type WorkflowRequest struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
	DAG     DAG             `json:"dag"`
}

// DAG represents the graph structure composed of individual tasks.
type DAG struct {
	Tasks map[string]TaskDefinition `json:"tasks"`
}

// TaskDefinition defines the properties of a workflow task, including its execution
// environment (worker type), task-specific parameters (input_data), and dependency keys.
type TaskDefinition struct {
	Worker       string          `json:"worker"` // "java" or "node"
	InputData    json.RawMessage `json:"input_data"`
	Dependencies []string        `json:"dependencies"`
	Compensation string          `json:"compensation,omitempty"`
}
