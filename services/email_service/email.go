package emailservice

import (
	"context"
	"log"
	"notifyx/models"
)

type EmailNotifier struct {
	smtpHost string
	smtpPort int
	username string
	password string
}

func NewEmailNotifier(host string, port int, username, password string) *EmailNotifier {
	return &EmailNotifier{
		smtpHost: host,
		smtpPort: port,
		username: username,
		password: password,
	}
}

func (e *EmailNotifier) Process(ctx context.Context, task *models.Task) error {
	// Extract email-specific data from task
	to := task.Payload["to"].(string)
	subject := task.Payload["subject"].(string)
	body := task.Payload["body"].(string)

	// Send email logic using net/smtp
	log.Printf("Sending email to %s: %s\n%s", to, subject, body)

	return nil
}
