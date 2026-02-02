package main

import (
	"fmt"
	"log"
	"notifyx/database"
	"notifyx/models"
	es "notifyx/services/email_service"
	"notifyx/services/queuer"
	"notifyx/services/server"
	ts "notifyx/services/telegram_service"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	// Database configuration from environment variables
	dbHost := getEnv("DB_HOST", "127.0.0.1")
	dbPort := getEnv("DB_PORT", "3306")
	dbUser := getEnv("DB_USER", "user")
	dbPass := getEnv("DB_PASSWORD", "password")
	dbName := getEnv("DB_NAME", "notifyx")

	// Worker configuration
	workerCount := getEnvAsInt("WORKER_COUNT", 10)
	bufferSize := getEnvAsInt("BUFFER_SIZE", 100)

	// Telegram configuration
	telegramBotToken := getEnv("TELEGRAM_BOT_TOKEN", "your_bot_token")

	// Email configuration
	smtpHost := getEnv("SMTP_HOST", "smtp.gmail.com")
	smtpPort := getEnvAsInt("SMTP_PORT", 587)
	smtpUsername := getEnv("SMTP_USERNAME", "user@example.com")
	smtpPassword := getEnv("SMTP_PASSWORD", "password")

	// Build database URL
	database_url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
		dbUser, dbPass, dbHost, dbPort, dbName)

	// Initialize database with retry logic
	if err := database.InitSQL(database_url); err != nil {
		log.Fatalf("Error initializing database: %v\n", err)
	}
	defer database.DB.Close()

	log.Println("Successfully connected to database")

	// Initialize notifiers with environment variables
	notifiers := map[models.TaskType]queuer.Notifier{
		models.TaskTypeTelegram: ts.NewTelegramNotifier(telegramBotToken),
		models.TaskTypeEmail:    es.NewEmailNotifier(smtpHost, smtpPort, smtpUsername, smtpPassword),
	}

	// Initialize and start task queue
	tq := queuer.InitTaskQueue(database.DB, workerCount, bufferSize, notifiers)
	tq.Start()

	router := server.InitServer(tq)
	router.Run(":8080")
	
	log.Printf("Notifyx service started with %d workers\n", workerCount)

	// Example tasks (optional - remove in production or make conditional)
	if getEnv("RUN_TEST_TASKS", "false") == "true" {
		telegramTask := &models.Task{
			ID:   "1",
			Type: models.TaskTypeTelegram,
			Payload: map[string]interface{}{
				"chat_id": "123456789",
				"message": "Hello from Telegram!",
			},
			MaxRetries: 3,
		}

		emailTask := &models.Task{
			ID:   "2",
			Type: models.TaskTypeEmail,
			Payload: map[string]interface{}{
				"to":      "user@example.com",
				"subject": "Test Email",
				"body":    "Hello from Email!",
			},
			MaxRetries: 3,
		}

		tq.Enqueue(telegramTask)
		tq.Enqueue(emailTask)
		log.Println("Test tasks enqueued")
	}

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Block until signal received
	sig := <-sigChan
	log.Printf("Received signal: %v. Shutting down gracefully...\n", sig)

	// Stop the task queue
	// tq.Stop()

	log.Println("Notifyx service stopped")
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// getEnvAsInt retrieves an environment variable as int or returns a default value
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Printf("Warning: Invalid integer value for %s, using default: %d\n", key, defaultValue)
		return defaultValue
	}
	return value
}
