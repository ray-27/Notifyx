package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

func InitSQL(dsn string) error {
	var err error
	maxRetries := 10
	retryDelay := 5 * time.Second

	for i := 0; i < maxRetries; i++ {
		DB, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to open MySQL connection: %v. Retrying in %v...",
				i+1, maxRetries, err, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		// Configure connection pool
		DB.SetMaxOpenConns(25)
		DB.SetMaxIdleConns(5)
		DB.SetConnMaxLifetime(5 * time.Minute)
		DB.SetConnMaxIdleTime(10 * time.Minute)

		// Verify connection
		err = DB.Ping()
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to ping MySQL: %v. Retrying in %v...",
				i+1, maxRetries, err, retryDelay)
			DB.Close()
			time.Sleep(retryDelay)
			continue
		}

		log.Println("---------------------------------------------")
		log.Println("Successfully established database connection")

		// Initialize database schema
		if err := initializeSchema(DB); err != nil {
			log.Printf("Failed to initialize schema: %v", err)
			DB.Close()
			return fmt.Errorf("schema initialization failed: %w", err)
		}

		log.Println("Database schema initialized successfully")
		log.Println("---------------------------------------------")
		return nil
	}

	return fmt.Errorf("failed to connect to MySQL after %d attempts: %w", maxRetries, err)
}

// initializeSchema creates all required tables and indexes
func initializeSchema(db *sql.DB) error {
	log.Println("Initializing database schema...")

	// Create migrations tracking table first
	if err := createMigrationsTable(db); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Run all migrations
	migrations := []Migration{
		{Version: 1, Name: "create_tasks_table", Up: createTasksTable},
		{Version: 2, Name: "create_task_executions_table", Up: createTaskExecutionsTable},
		{Version: 3, Name: "create_dead_letter_queue_table", Up: createDeadLetterQueueTable},
		{Version: 4, Name: "create_task_statistics_table", Up: createTaskStatisticsTable},
	}

	for _, migration := range migrations {
		if err := runMigration(db, migration); err != nil {
			return fmt.Errorf("migration %d (%s) failed: %w", migration.Version, migration.Name, err)
		}
	}

	log.Println("All migrations completed successfully")
	return nil
}

// runMigration executes a migration if it hasn't been applied yet
func runMigration(db *sql.DB, migration Migration) error {
	// Check if migration already applied
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version = ?", migration.Version).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check migration status: %w", err)
	}

	if count > 0 {
		log.Printf("Migration %d (%s) already applied, skipping", migration.Version, migration.Name)
		return nil
	}

	log.Printf("Running migration %d: %s", migration.Version, migration.Name)

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Execute migration
	if err := migration.Up(db); err != nil {
		return fmt.Errorf("failed to execute migration: %w", err)
	}

	// Record migration
	_, err = tx.Exec("INSERT INTO schema_migrations (version, name) VALUES (?, ?)", migration.Version, migration.Name)
	if err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration: %w", err)
	}

	log.Printf("Migration %d (%s) completed successfully", migration.Version, migration.Name)
	return nil
}

// Close closes the database connection
func Close() error {
	if DB != nil {
		log.Println("Closing database connection...")
		return DB.Close()
	}
	return nil
}

// HealthCheck verifies database connectivity
func HealthCheck() error {
	if DB == nil {
		return fmt.Errorf("database connection is nil")
	}

	if err := DB.Ping(); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	return nil
}

// GetStats returns database connection statistics
func GetStats() sql.DBStats {
	if DB == nil {
		return sql.DBStats{}
	}
	return DB.Stats()
}
