package dag

import (
	"reddo/gate/internal/models"
	"testing"
)

func TestValidateDAG(t *testing.T) {
	tests := []struct {
		name        string
		dag         models.DAG
		expectError bool
		expectedLen int
	}{
		{
			name: "Valid Linear DAG",
			dag: models.DAG{
				Tasks: map[string]models.TaskDefinition{
					"task_a": {Worker: "java", Dependencies: []string{}},
					"task_b": {Worker: "node", Dependencies: []string{"task_a"}},
					"task_c": {Worker: "java", Dependencies: []string{"task_b"}},
				},
			},
			expectError: false,
			expectedLen: 3,
		},
		{
			name: "Valid Fork-Join DAG",
			dag: models.DAG{
				Tasks: map[string]models.TaskDefinition{
					"task_a": {Worker: "java", Dependencies: []string{}},
					"task_b": {Worker: "node", Dependencies: []string{"task_a"}},
					"task_c": {Worker: "java", Dependencies: []string{"task_a"}},
					"task_d": {Worker: "node", Dependencies: []string{"task_b", "task_c"}},
				},
			},
			expectError: false,
			expectedLen: 4,
		},
		{
			name: "Circular Dependency - Self Reference",
			dag: models.DAG{
				Tasks: map[string]models.TaskDefinition{
					"task_a": {Worker: "java", Dependencies: []string{"task_a"}},
				},
			},
			expectError: true,
		},
		{
			name: "Circular Dependency - Multi Node Cycle",
			dag: models.DAG{
				Tasks: map[string]models.TaskDefinition{
					"task_a": {Worker: "java", Dependencies: []string{"task_c"}},
					"task_b": {Worker: "node", Dependencies: []string{"task_a"}},
					"task_c": {Worker: "java", Dependencies: []string{"task_b"}},
				},
			},
			expectError: true,
		},
		{
			name: "Orphan Dependency",
			dag: models.DAG{
				Tasks: map[string]models.TaskDefinition{
					"task_a": {Worker: "java", Dependencies: []string{"non_existent_task"}},
				},
			},
			expectError: true,
		},
		{
			name: "Empty DAG",
			dag: models.DAG{
				Tasks: map[string]models.TaskDefinition{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted, err := ValidateDAG(tt.dag)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateDAG() error = %v, expectError %v", err, tt.expectError)
				return
			}
			if !tt.expectError {
				if len(sorted) != tt.expectedLen {
					t.Errorf("ValidateDAG() returned sorted list length %d, expected %d", len(sorted), tt.expectedLen)
				}
				// Verify topological sorting order: dependencies must appear before their dependents
				positions := make(map[string]int)
				for idx, key := range sorted {
					positions[key] = idx
				}
				for key, task := range tt.dag.Tasks {
					for _, dep := range task.Dependencies {
						if positions[dep] >= positions[key] {
							t.Errorf("Topological sort failed: dependency %q appears after or at dependent %q", dep, key)
						}
					}
				}
			}
		})
	}
}
