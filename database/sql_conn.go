package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

// InitSQL initializes the global MySQL connection with retry logic.
// Example dsn: "user:password@tcp(127.0.0.1:3306)/dbname?parseTime=true&charset=utf8mb4"
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

		// Verify connection
		err = DB.Ping()
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to ping MySQL: %v. Retrying in %v...",
				i+1, maxRetries, err, retryDelay)
			DB.Close()
			time.Sleep(retryDelay)
			continue
		}

		log.Println("Successfully established database connection")
		return nil
	}

	return fmt.Errorf("failed to connect to MySQL after %d attempts: %w", maxRetries, err)
}

// Close closes the database connection
func Close() error {
	if DB != nil {
		return DB.Close()
	}
	return nil
}
