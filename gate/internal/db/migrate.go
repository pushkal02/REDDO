package db

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"sort"
)

//go:embed patches/*.sql
var migrationFS embed.FS

// RunMigrations checks the schema_migrations table and executes any unapplied SQL patches
// found in the embedded patches directory in numerical/alphabetical order.
func RunMigrations(db *sql.DB) error {
	log.Println("[Migrations] Starting database auto-migrations...")

	// Ensure the migration tracking table exists
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// Read files from the embedded FS
	entries, err := fs.ReadDir(migrationFS, "patches")
	if err != nil {
		return fmt.Errorf("failed to read embedded migrations directory: %w", err)
	}

	// Filter and sort migrations
	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	for _, file := range migrationFiles {
		applied, err := isMigrationApplied(db, file)
		if err != nil {
			return fmt.Errorf("failed to check migration status for %s: %w", file, err)
		}

		if applied {
			log.Printf("[Migrations] Patch %s already applied.", file)
			continue
		}

		log.Printf("[Migrations] Applying patch %s...", file)
		content, err := migrationFS.ReadFile("patches/" + file)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", file, err)
		}

		// Execute migration in a transaction
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start transaction for %s: %w", file, err)
		}
		// Defer rollback in case of panic or error (ignored if committed)
		defer tx.Rollback()

		if _, err := tx.Exec(string(content)); err != nil {
			return fmt.Errorf("failed to execute SQL for patch %s: %w", file, err)
		}

		if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES ($1);", file); err != nil {
			return fmt.Errorf("failed to record patch execution for %s: %w", file, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction for patch %s: %w", file, err)
		}

		log.Printf("[Migrations] Successfully applied patch %s.", file)
	}

	log.Println("[Migrations] All database migrations are up to date.")
	return nil
}

// isMigrationApplied checks if a migration patch has already been registered in the database.
func isMigrationApplied(db *sql.DB, version string) (bool, error) {
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1);", version).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
