package db

import (
	"database/sql"
	"log"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // SQL driver wrapper for pgx
	"reddo/gate/internal/config"
)

// Connect establishes a connection pool to the PostgreSQL database.
// It configures connection pooling settings to prevent socket exhaustion and maintain performance under load.
func Connect(cfg *config.Config) (*sql.DB, error) {
	log.Printf("[Database] Connecting to PostgreSQL at %s...", redactURL(cfg.DatabaseURL))

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}

	// Set connection pool limits:
	// - MaxOpenConns: Maximum number of open connections to the database. Prevents connection exhaustion on the PG server.
	// - MaxIdleConns: Maximum number of connections in the idle connection pool.
	// - ConnMaxLifetime: Maximum amount of time a connection may be reused. Avoids stale connections.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify the connection is active
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	log.Println("[Database] Successfully connected to PostgreSQL.")
	return db, nil
}

// redactURL hides the password in connection strings for secure logging.
func redactURL(url string) string {
	const schemeSep = "://"
	schemeEnd := strings.Index(url, schemeSep)
	if schemeEnd == -1 {
		schemeEnd = 0
	} else {
		schemeEnd += len(schemeSep)
	}

	atIdx := strings.Index(url[schemeEnd:], "@")
	if atIdx == -1 {
		return url
	}
	atIdx += schemeEnd

	colonIdx := strings.Index(url[schemeEnd:atIdx], ":")
	if colonIdx == -1 {
		return url
	}
	colonIdx += schemeEnd

	return url[:colonIdx] + ":****" + url[atIdx:]
}
