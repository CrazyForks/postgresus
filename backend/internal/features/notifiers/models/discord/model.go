package discord_notifier

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

type DiscordNotifier struct {
	NotifierID        uuid.UUID `json:"notifierId" gorm:"primaryKey;column:notifier_id"`
	ChannelWebhookURL string    `json:"channelWebhookUrl" gorm:"not null;column:channel_webhook_url"`
}

func (d *DiscordNotifier) TableName() string {
	return "discord_notifiers"
}

func (d *DiscordNotifier) Validate() error {
	if d.ChannelWebhookURL == "" {
		return errors.New("webhook URL is required")
	}

	return nil
}

func (d *DiscordNotifier) Send(logger *slog.Logger, heading string, message string) error {
	fullMessage := heading
	if message != "" {
		fullMessage = fmt.Sprintf("%s\n\n%s", heading, message)
	}

	payload := map[string]interface{}{
		"content": fullMessage,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal Discord payload: %w", err)
	}

	req, err := http.NewRequest("POST", d.ChannelWebhookURL, bytes.NewReader(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send Discord message: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf(
			"discord API returned non-OK status: %s. Error: %s",
			resp.Status,
			string(bodyBytes),
		)
	}

	return nil
}
