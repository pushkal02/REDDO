// Package handlers defines HTTP handler functions for the Fiber web framework.
package handlers

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"reddo/gate/internal/dag"
	"reddo/gate/internal/models"
	"reddo/gate/internal/mq"
)

// WorkflowHandler coordinates HTTP workflow requests, database records, and queue tasks.
type WorkflowHandler struct {
	db *sql.DB
	mq *mq.Client
}

// NewWorkflowHandler returns a pointer to a initialized WorkflowHandler.
func NewWorkflowHandler(db *sql.DB, mqClient *mq.Client) *WorkflowHandler {
	return &WorkflowHandler{
		db: db,
		mq: mqClient,
	}
}

func logTrace(level, correlationID, requestID, format string, v ...interface{}) {
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	msg := fmt.Sprintf(format, v...)
	log.Printf("[%s] [%s] [gate] [%s] [%s] - %s", ts, level, correlationID, requestID, msg)
}

// SubmitWorkflow handles incoming POST requests containing DAG workflow definitions.
// It parses the body, validates dependencies/cycles, persists records, and triggers execution.
func (h *WorkflowHandler) SubmitWorkflow(c *fiber.Ctx) error {
	var req models.WorkflowRequest
	if err := c.BodyParser(&req); err != nil {
		logTrace("ERROR", "-", "-", "invalid JSON payload: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid JSON payload: %v", err),
		})
	}

	// Validate required fields
	if req.ID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "workflow 'id' is required"})
	}
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "workflow 'name' is required"})
	}
	if len(req.DAG.Tasks) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "workflow 'dag.tasks' must contain at least one task"})
	}

	// Extract or generate CorrelationID and RequestID
	correlationID := c.Get("X-Correlation-ID")
	if correlationID == "" {
		correlationID = req.ID
	}
	requestID := c.Get("X-Request-ID")
	if requestID == "" {
		requestID = generateUUID()
	}

	// Set headers for tracing responses
	c.Set("X-Correlation-ID", correlationID)
	c.Set("X-Request-ID", requestID)

	// Step 1: Validate DAG using Kahn's algorithm for cycles and orphan dependencies.
	_, err := dag.ValidateDAG(req.DAG)
	if err != nil {
		logTrace("WARN", correlationID, requestID, "Rejecting invalid DAG %q: %v", req.ID, err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("DAG validation failed: %v", err),
		})
	}

	// Pre-generate execution IDs for all tasks so we can store them and map dependencies.
	taskExecIDs := make(map[string]string)
	for taskKey := range req.DAG.Tasks {
		taskExecIDs[taskKey] = generateUUID()
	}

	// Step 2: Persist state in a database transaction.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		logTrace("ERROR", correlationID, requestID, "Failed to start DB transaction: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal database error"})
	}
	defer tx.Rollback()

	// Insert Workflow Instance (Status: RUNNING because execution starts immediately)
	dagJSON, _ := json.Marshal(req.DAG)
	payloadJSON, _ := json.Marshal(req.Payload)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO workflow_instances (id, name, status, dag, payload)
		VALUES ($1, $2, $3, $4, $5);
	`, req.ID, req.Name, "RUNNING", dagJSON, payloadJSON)
	if err != nil {
		logTrace("ERROR", correlationID, requestID, "Database insert failed for workflow %s: %v", req.ID, err)
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": fmt.Sprintf("workflow instance with ID %q already exists", req.ID),
		})
	}

	// Insert Task Executions (Initially status PENDING)
	var rootTasks []string
	for taskKey, taskDef := range req.DAG.Tasks {
		execID := taskExecIDs[taskKey]
		status := "PENDING"
		
		// If task has 0 dependencies, it is a root task that runs immediately.
		isRoot := len(taskDef.Dependencies) == 0
		if isRoot {
			status = "RUNNING"
			rootTasks = append(rootTasks, taskKey)
		}

		inputJSON, _ := json.Marshal(taskDef.InputData)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO task_executions (id, workflow_instance_id, task_key, status, input_data)
			VALUES ($1, $2, $3, $4, $5);
		`, execID, req.ID, taskKey, status, inputJSON)
		if err != nil {
			logTrace("ERROR", correlationID, requestID, "Failed to insert task execution record: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to record task state"})
		}
	}

	// Step 3: Commit DB transaction BEFORE publishing messages.
	if err := tx.Commit(); err != nil {
		logTrace("ERROR", correlationID, requestID, "Failed to commit database transaction: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to commit workflow state"})
	}

	// Step 4: Publish root tasks to RabbitMQ.
	var publishErrors []string
	for _, taskKey := range rootTasks {
		taskDef := req.DAG.Tasks[taskKey]
		execID := taskExecIDs[taskKey]

		msg := mq.TaskMessage{
			TaskExecutionID:    execID,
			WorkflowInstanceID: req.ID,
			TaskKey:            taskKey,
			InputData:          taskDef.InputData,
			CorrelationID:      correlationID,
			RequestID:          requestID,
		}

		err = h.mq.PublishTask(ctx, taskDef.Worker, msg)
		if err != nil {
			errStr := fmt.Sprintf("failed to queue task %q: %v", taskKey, err)
			logTrace("ERROR", correlationID, requestID, "%s", errStr)
			publishErrors = append(publishErrors, errStr)
		}
	}

	if len(publishErrors) > 0 {
		return c.Status(fiber.StatusMultiStatus).JSON(fiber.Map{
			"message": "workflow registered, but some initial tasks failed to queue",
			"errors":  publishErrors,
		})
	}

	logTrace("INFO", correlationID, requestID, "Workflow %q started successfully with %d root tasks.", req.ID, len(rootTasks))
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"message":              "workflow submitted and execution started",
		"workflow_instance_id": req.ID,
	})
}

// generateUUID creates a cryptographic UUID v4 string without external dependencies.
func generateUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// Fallback to timestamp+random in the extreme case of entropy exhaustion
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	// Conform to UUID version 4 variant 1 format
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
