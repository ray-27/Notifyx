package telegramservice

import (
	"context"
	"log"
	"notifyx/models"
)

type TelegramNotifier struct {
	botToken string
	apiURL   string
	// add other telegram-specific fields
}

func NewTelegramNotifier(botToken string) *TelegramNotifier {
	return &TelegramNotifier{
		botToken: botToken,
		apiURL:   "https://api.telegram.org",
	}
}

func (t *TelegramNotifier) Process(ctx context.Context, task *models.Task) error {
	// Extract telegram-specific data from task
	chatID := task.Payload["chat_id"].(string)
	message := task.Payload["message"].(string)

	// Send telegram message logic
	// Make HTTP POST to Telegram API
	log.Printf("Sending Telegram message to %s: %s", chatID, message)

	return nil
}
