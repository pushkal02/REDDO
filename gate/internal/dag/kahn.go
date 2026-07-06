// Package dag implements Directed Acyclic Graph validation and sorting routines.
package dag

import (
	"fmt"
	"reddo/gate/internal/models"
)

// ValidateDAG runs Kahn's Algorithm to check for circular dependencies
// and verifies that all dependencies exist (detects orphan dependencies).
// It returns a topologically sorted list of task keys if the DAG is valid, or an error.
func ValidateDAG(dag models.DAG) ([]string, error) {
	numTasks := len(dag.Tasks)
	if numTasks == 0 {
		return nil, fmt.Errorf("empty DAG: at least one task is required")
	}

	// Step 1: Pre-validate orphan dependencies (dependencies referencing undefined tasks)
	// and initialize Graph representation maps.
	inDegrees := make(map[string]int)
	adjList := make(map[string][]string)

	// Explicitly initialize in-degrees for all defined tasks to 0
	for taskKey := range dag.Tasks {
		inDegrees[taskKey] = 0
	}

	for taskKey, taskDef := range dag.Tasks {
		for _, dep := range taskDef.Dependencies {
			// Check if the dependency exists in the DAG definition
			if _, exists := dag.Tasks[dep]; !exists {
				return nil, fmt.Errorf("orphan dependency: task %q depends on undefined task %q", taskKey, dep)
			}
			// There is a directed edge: dep -> taskKey (dep must execute before taskKey)
			adjList[dep] = append(adjList[dep], taskKey)
			inDegrees[taskKey]++
		}
	}

	// Step 2: Initialize queue with all tasks that have in-degree of 0 (no dependencies).
	// These are the root tasks that can be executed immediately.
	var queue []string
	for taskKey, degree := range inDegrees {
		if degree == 0 {
			queue = append(queue, taskKey)
		}
	}

	// Step 3: Process the queue using BFS-like traversal
	var sortedList []string
	for len(queue) > 0 {
		// Dequeue task
		curr := queue[0]
		queue = queue[1:]
		sortedList = append(sortedList, curr)

		// For each outgoing neighbor, decrement its in-degree
		for _, neighbor := range adjList[curr] {
			inDegrees[neighbor]--
			// If in-degree becomes 0, all dependencies for this task are satisfied,
			// so it is ready for execution. Push it to the queue.
			if inDegrees[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	// Step 4: If the sorted list length does not match the total task count,
	// a cycle must exist because some tasks were never queued (in-degree never reached 0).
	if len(sortedList) != numTasks {
		// Identify which tasks are part of the cycle for better error reporting
		var cyclicTasks []string
		for taskKey, degree := range inDegrees {
			if degree > 0 {
				cyclicTasks = append(cyclicTasks, taskKey)
			}
		}
		return nil, fmt.Errorf("circular dependency detected among tasks: %v", cyclicTasks)
	}

	return sortedList, nil
}
