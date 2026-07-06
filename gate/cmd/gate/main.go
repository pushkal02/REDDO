package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"reddo/gate/internal/config"
	"reddo/gate/internal/db"
	"reddo/gate/internal/handlers"
	"reddo/gate/internal/mq"
)

func main() {
	log.Println("[Gate] Starting RESILIENT EVENT-DRIVEN DAG ORCHESTRATOR API Gateway...")

	// 1. Load configuration
	cfg := config.Load()

	// 2. Connect to Database (Postgres)
	dbConn, err := db.Connect(cfg)
	if err != nil {
		log.Fatalf("[Gate] Critical: Failed to connect to database: %v", err)
	}
	defer func() {
		log.Println("[Gate] Closing database connections...")
		dbConn.Close()
	}()

	// 3. Run Auto-Migrations
	if err := db.RunMigrations(dbConn); err != nil {
		log.Fatalf("[Gate] Critical: Migration run failed: %v", err)
	}

	// 4. Connect to RabbitMQ & setup queues
	mqClient, err := mq.NewClient(cfg)
	if err != nil {
		log.Fatalf("[Gate] Critical: Failed to connect to message broker: %v", err)
	}
	defer func() {
		log.Println("[Gate] Closing message broker connections...")
		mqClient.Close()
	}()

	// 5. Initialize Web Framework (Fiber)
	app := fiber.New(fiber.Config{
		DisableStartupMessage: false,
		AppName:               "REDDO-Gate v1.0",
	})

	// 6. Setup handlers and routes
	workflowHandler := handlers.NewWorkflowHandler(dbConn, mqClient)

	// Liveness and Readiness probe for Kubernetes
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status": "UP",
			"time":   time.Now().Format(time.RFC3339),
		})
	})

	// Main ingress endpoint for workflows
	app.Post("/api/v1/workflows", workflowHandler.SubmitWorkflow)

	// 7. Setup Graceful Shutdown channel
	// Listening for OS signals to allow outstanding requests/transactions to complete before termination.
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := app.Listen(cfg.Port); err != nil {
			log.Printf("[Gate] Server stopped listening: %v", err)
		}
	}()

	log.Printf("[Gate] Gateway service listening on port %s", cfg.Port)

	// Block until signal is received
	sig := <-shutdownChan
	log.Printf("[Gate] Termination signal received (%v). Initiating graceful shutdown...", sig)

	// Set a deadline timeout for graceful shutdown (e.g., 10 seconds)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := app.ShutdownWithContext(ctx); err != nil {
		log.Printf("[Gate] Error shutting down Fiber app: %v", err)
	}

	log.Println("[Gate] Service terminated successfully.")
}
