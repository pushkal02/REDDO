// Package config manages application configuration loaded from environment variables.
// It provides fallback defaults suitable for local development.
package config

import (
	"os"
	"strings"
)

// Config holds all configuration parameters for the Gate service.
type Config struct {
	// Port is the HTTP server bind address (e.g., ":8080").
	Port string
	// DatabaseURL is the PostgreSQL connection string.
	DatabaseURL string
	// RabbitMQURL is the AMQP connection string.
	RabbitMQURL string
}

// Load fetches configurations from the environment, applying sane defaults where needed.
// This ensures the service runs out-of-the-box locally, but is easily configurable via K8s ConfigMaps/Secrets.
func Load() *Config {
	port := getEnv("PORT", "8080")
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	return &Config{
		Port:        port,
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/reddo?sslmode=disable"),
		RabbitMQURL: getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
	}
}

// getEnv retrieves an environment variable or returns the default value if unset.
func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
