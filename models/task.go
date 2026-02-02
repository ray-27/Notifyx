package models

import "time"

type TaskType string

const (
	TaskTypeTelegram TaskType = "telegram"
	TaskTypeEmail    TaskType = "email"
)

type Task struct {
	ID         string
	Type       TaskType
	Payload    map[string]interface{} // flexible payload for different notification types
	RetryCount int
	MaxRetries int
	Status     string
	CreatedAt  time.Time
}
