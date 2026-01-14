package backups_download

import (
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type DownloadTokenService struct {
	repository      *DownloadTokenRepository
	logger          *slog.Logger
	downloadTracker *DownloadTracker
}

func (s *DownloadTokenService) Generate(backupID, userID uuid.UUID) (string, error) {
	token := GenerateSecureToken()

	downloadToken := &DownloadToken{
		Token:     token,
		BackupID:  backupID,
		UserID:    userID,
		ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
		Used:      false,
	}

	if err := s.repository.Create(downloadToken); err != nil {
		return "", err
	}

	s.logger.Info("Generated download token", "backupId", backupID, "userId", userID)
	return token, nil
}

func (s *DownloadTokenService) ValidateAndConsume(token string) (*DownloadToken, error) {
	dt, err := s.repository.FindByToken(token)
	if err != nil {
		return nil, err
	}

	if dt == nil {
		return nil, errors.New("invalid token")
	}

	if dt.Used {
		return nil, errors.New("token already used")
	}

	if time.Now().UTC().After(dt.ExpiresAt) {
		return nil, errors.New("token expired")
	}

	if err := s.downloadTracker.AcquireDownloadLock(dt.UserID); err != nil {
		return nil, err
	}

	dt.Used = true
	if err := s.repository.Update(dt); err != nil {
		s.logger.Error("Failed to mark token as used", "error", err)
	}

	s.logger.Info("Token validated and consumed", "backupId", dt.BackupID, "userId", dt.UserID)
	return dt, nil
}

func (s *DownloadTokenService) RefreshDownloadLock(userID uuid.UUID) {
	s.downloadTracker.RefreshDownloadLock(userID)
}

func (s *DownloadTokenService) ReleaseDownloadLock(userID uuid.UUID) {
	s.downloadTracker.ReleaseDownloadLock(userID)
	s.logger.Info("Released download lock", "userId", userID)
}

func (s *DownloadTokenService) IsDownloadInProgress(userID uuid.UUID) bool {
	return s.downloadTracker.IsDownloadInProgress(userID)
}

func (s *DownloadTokenService) CleanExpiredTokens() error {
	now := time.Now().UTC()
	if err := s.repository.DeleteExpired(now); err != nil {
		return err
	}
	s.logger.Debug("Cleaned expired download tokens")
	return nil
}
