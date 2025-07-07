package notifiers

import (
	webhook_notifier "postgresus-backend/internal/features/notifiers/models/webhook"

	"github.com/google/uuid"
)

func CreateTestNotifier(userID uuid.UUID) *Notifier {
	notifier := &Notifier{
		UserID:       userID,
		Name:         "test " + uuid.New().String(),
		NotifierType: NotifierTypeWebhook,
		WebhookNotifier: &webhook_notifier.WebhookNotifier{
			WebhookURL:    "https://webhook.site/123e4567-e89b-12d3-a456-426614174000",
			WebhookMethod: webhook_notifier.WebhookMethodPOST,
		},
	}

	notifier, err := notifierRepository.Save(notifier)
	if err != nil {
		panic(err)
	}

	return notifier
}

func RemoveTestNotifier(notifier *Notifier) {
	err := notifierRepository.Delete(notifier)
	if err != nil {
		panic(err)
	}
}
