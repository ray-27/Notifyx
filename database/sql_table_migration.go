package database

import (
	"database/sql"
	"fmt"
	"log"
)

// Migration represents a database migration
type Migration struct {
	Version int
	Name    string
	Up      func(*sql.DB) error
}

// createMigrationsTable creates a table to track applied migrations
func createMigrationsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT NOT NULL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			INDEX idx_applied_at (applied_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`

	_, err := db.Exec(query)
	return err
}

// createTasksTable creates the main tasks table
func createTasksTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS tasks (
			-- Primary Key
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

			-- Task Identification
			task_id VARCHAR(255) NOT NULL UNIQUE COMMENT 'External task identifier (UUID)',
			task_type ENUM('telegram', 'email', 'sms', 'push', 'webhook') NOT NULL COMMENT 'Type of notification',

			-- Task Status
			status ENUM(
				'pending',
				'processing',
				'completed',
				'failed',
				'dead_letter',
				'cancelled'
			) NOT NULL DEFAULT 'pending',

			-- Priority (for future priority queue implementation)
			priority TINYINT UNSIGNED NOT NULL DEFAULT 5 COMMENT '1=highest, 10=lowest',

			-- Retry Management
			retry_count SMALLINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Current retry attempt',
			max_retries SMALLINT UNSIGNED NOT NULL DEFAULT 3 COMMENT 'Maximum retry attempts',

			-- Payload (notification data)
			payload JSON NOT NULL COMMENT 'Task payload containing notification details',

			-- Error Tracking
			last_error TEXT NULL COMMENT 'Last error message',
			error_count SMALLINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Total number of errors',

			-- Worker Tracking
			worker_id VARCHAR(100) NULL COMMENT 'ID of worker currently processing',
			locked_at TIMESTAMP NULL COMMENT 'When task was locked by worker',
			locked_until TIMESTAMP NULL COMMENT 'Lock expiration time',

			-- Scheduling
			scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When task should be processed',
			next_retry_at TIMESTAMP NULL COMMENT 'When to retry if failed',

			-- Timestamps
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			started_at TIMESTAMP NULL COMMENT 'When processing started',
			completed_at TIMESTAMP NULL COMMENT 'When task completed/failed',

			-- Soft Delete
			deleted_at TIMESTAMP NULL COMMENT 'Soft delete timestamp',

			-- Metadata
			metadata JSON NULL COMMENT 'Additional metadata (user_id, tenant_id, etc.)',

			-- Indexes for performance
			INDEX idx_status_scheduled (status, scheduled_at),
			INDEX idx_task_type_status (task_type, status),
			INDEX idx_next_retry (status, next_retry_at),
			INDEX idx_created_at (created_at),
			INDEX idx_locked_until (locked_until),
			INDEX idx_worker_id (worker_id),
			INDEX idx_priority_status (priority, status, scheduled_at),
			INDEX idx_deleted_at (deleted_at),
			INDEX idx_worker_selection (status, priority, scheduled_at, locked_until)

		) ENGINE=InnoDB
		  DEFAULT CHARSET=utf8mb4
		  COLLATE=utf8mb4_unicode_ci
		  COMMENT='Main task queue table for Notifyx'
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create tasks table: %w", err)
	}

	log.Println("✓ Tasks table created successfully")
	return nil
}

// createTaskExecutionsTable creates the task executions audit table
func createTaskExecutionsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS task_executions (
			-- Primary Key
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

			-- Foreign Key to tasks
			task_id BIGINT UNSIGNED NOT NULL,

			-- Execution Details
			attempt_number SMALLINT UNSIGNED NOT NULL COMMENT 'Retry attempt number',
			worker_id VARCHAR(100) NOT NULL COMMENT 'Worker that processed this attempt',

			-- Status
			status ENUM('started', 'success', 'failed', 'timeout') NOT NULL,

			-- Timing
			started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP NULL,
			duration_ms INT UNSIGNED NULL COMMENT 'Execution duration in milliseconds',

			-- Error Details
			error_message TEXT NULL,
			error_type VARCHAR(100) NULL COMMENT 'Error classification (network, timeout, etc.)',
			stack_trace TEXT NULL,

			-- Response Data
			response_data JSON NULL COMMENT 'Response from external service',

			-- Metadata
			metadata JSON NULL COMMENT 'Execution context (IP, host, etc.)',

			-- Indexes
			INDEX idx_task_id (task_id),
			INDEX idx_worker_id (worker_id),
			INDEX idx_status (status),
			INDEX idx_started_at (started_at),

			-- Foreign Key Constraint
			FOREIGN KEY (task_id) REFERENCES tasks(id)
				ON DELETE CASCADE
				ON UPDATE CASCADE

		) ENGINE=InnoDB
		  DEFAULT CHARSET=utf8mb4
		  COLLATE=utf8mb4_unicode_ci
		  COMMENT='Audit trail for task execution attempts'
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create task_executions table: %w", err)
	}

	log.Println("✓ Task executions table created successfully")
	return nil
}

// createDeadLetterQueueTable creates the dead letter queue table
func createDeadLetterQueueTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS dead_letter_queue (
			-- Primary Key
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

			-- Original Task Data
			original_task_id BIGINT UNSIGNED NOT NULL,
			task_id VARCHAR(255) NOT NULL,
			task_type ENUM('telegram', 'email', 'sms', 'push', 'webhook') NOT NULL,
			payload JSON NOT NULL,

			-- Failure Details
			failure_reason TEXT NOT NULL COMMENT 'Why this task ended up in DLQ',
			total_attempts SMALLINT UNSIGNED NOT NULL,
			last_error TEXT NULL,

			-- Resolution
			resolved BOOLEAN NOT NULL DEFAULT FALSE,
			resolved_at TIMESTAMP NULL,
			resolved_by VARCHAR(100) NULL COMMENT 'User/system that resolved',
			resolution_notes TEXT NULL,

			-- Timestamps
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'When moved to DLQ',
			original_created_at TIMESTAMP NOT NULL COMMENT 'Original task creation time',

			-- Indexes
			INDEX idx_task_id (task_id),
			INDEX idx_original_task_id (original_task_id),
			INDEX idx_resolved (resolved),
			INDEX idx_created_at (created_at),
			INDEX idx_task_type (task_type)

		) ENGINE=InnoDB
		  DEFAULT CHARSET=utf8mb4
		  COLLATE=utf8mb4_unicode_ci
		  COMMENT='Dead letter queue for permanently failed tasks'
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create dead_letter_queue table: %w", err)
	}

	log.Println("✓ Dead letter queue table created successfully")
	return nil
}

// createTaskStatisticsTable creates the task statistics table
func createTaskStatisticsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS task_statistics (
			-- Primary Key
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

			-- Aggregation Period
			period_start TIMESTAMP NOT NULL,
			period_end TIMESTAMP NOT NULL,
			granularity ENUM('minute', 'hour', 'day') NOT NULL,

			-- Task Type
			task_type ENUM('telegram', 'email', 'sms', 'push', 'webhook') NOT NULL,

			-- Metrics
			tasks_created INT UNSIGNED NOT NULL DEFAULT 0,
			tasks_completed INT UNSIGNED NOT NULL DEFAULT 0,
			tasks_failed INT UNSIGNED NOT NULL DEFAULT 0,
			tasks_retried INT UNSIGNED NOT NULL DEFAULT 0,
			tasks_dead_letter INT UNSIGNED NOT NULL DEFAULT 0,

			-- Performance Metrics
			avg_duration_ms INT UNSIGNED NULL,
			min_duration_ms INT UNSIGNED NULL,
			max_duration_ms INT UNSIGNED NULL,
			p95_duration_ms INT UNSIGNED NULL COMMENT '95th percentile duration',
			p99_duration_ms INT UNSIGNED NULL COMMENT '99th percentile duration',

			-- Timestamps
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

			-- Indexes
			UNIQUE KEY unique_period (task_type, granularity, period_start),
			INDEX idx_period_start (period_start),
			INDEX idx_task_type (task_type)

		) ENGINE=InnoDB
		  DEFAULT CHARSET=utf8mb4
		  COLLATE=utf8mb4_unicode_ci
		  COMMENT='Aggregated task statistics for monitoring'
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create task_statistics table: %w", err)
	}

	log.Println("✓ Task statistics table created successfully")
	return nil
}
