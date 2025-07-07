package google_drive_storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

type GoogleDriveStorage struct {
	StorageID    uuid.UUID `json:"storageId"    gorm:"primaryKey;type:uuid;column:storage_id"`
	ClientID     string    `json:"clientId"     gorm:"not null;type:text;column:client_id"`
	ClientSecret string    `json:"clientSecret" gorm:"not null;type:text;column:client_secret"`
	TokenJSON    string    `json:"tokenJson"    gorm:"not null;type:text;column:token_json"`
}

func (s *GoogleDriveStorage) TableName() string {
	return "google_drive_storages"
}

func (s *GoogleDriveStorage) SaveFile(
	logger *slog.Logger,
	fileID uuid.UUID,
	file io.Reader,
) error {
	return s.withRetryOnAuth(func(driveService *drive.Service) error {
		ctx := context.Background()
		filename := fileID.String()

		// Delete any previous copy so we keep at most one object per logical file.
		_ = s.deleteByName(ctx, driveService, filename) // ignore "not found"

		fileMeta := &drive.File{Name: filename}

		_, err := driveService.Files.Create(fileMeta).Media(file).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("failed to upload file to Google Drive: %w", err)
		}

		logger.Info("file uploaded to Google Drive", "name", filename)
		return nil
	})
}

func (s *GoogleDriveStorage) GetFile(fileID uuid.UUID) (io.ReadCloser, error) {
	var result io.ReadCloser
	err := s.withRetryOnAuth(func(driveService *drive.Service) error {
		fileIDGoogle, err := s.lookupFileID(driveService, fileID.String())
		if err != nil {
			return err
		}

		resp, err := driveService.Files.Get(fileIDGoogle).Download()
		if err != nil {
			return fmt.Errorf("failed to download file from Google Drive: %w", err)
		}

		result = resp.Body
		return nil
	})

	return result, err
}

func (s *GoogleDriveStorage) DeleteFile(fileID uuid.UUID) error {
	return s.withRetryOnAuth(func(driveService *drive.Service) error {
		ctx := context.Background()
		return s.deleteByName(ctx, driveService, fileID.String())
	})
}

func (s *GoogleDriveStorage) Validate() error {
	switch {
	case s.ClientID == "":
		return errors.New("client ID is required")
	case s.ClientSecret == "":
		return errors.New("client secret is required")
	case s.TokenJSON == "":
		return errors.New("token JSON is required")
	}

	// Also validate that the token JSON contains a refresh token
	var token oauth2.Token
	if err := json.Unmarshal([]byte(s.TokenJSON), &token); err != nil {
		return fmt.Errorf("invalid token JSON format: %w", err)
	}

	if token.RefreshToken == "" {
		return errors.New("token JSON must contain a refresh token for automatic token refresh")
	}

	return nil
}

func (s *GoogleDriveStorage) TestConnection() error {
	return s.withRetryOnAuth(func(driveService *drive.Service) error {
		ctx := context.Background()
		testFilename := "test-connection-" + uuid.New().String()
		testData := []byte("test")

		// Test write operation
		fileMeta := &drive.File{Name: testFilename}
		file, err := driveService.Files.Create(fileMeta).
			Media(strings.NewReader(string(testData))).
			Context(ctx).
			Do()
		if err != nil {
			return fmt.Errorf("failed to write test file to Google Drive: %w", err)
		}

		// Test read operation
		resp, err := driveService.Files.Get(file.Id).Download()
		if err != nil {
			// Clean up test file before returning error
			_ = driveService.Files.Delete(file.Id).Context(ctx).Do()
			return fmt.Errorf("failed to read test file from Google Drive: %w", err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("failed to close response body: %v\n", err)
			}
		}()

		readData, err := io.ReadAll(resp.Body)
		if err != nil {
			// Clean up test file before returning error
			_ = driveService.Files.Delete(file.Id).Context(ctx).Do()
			return fmt.Errorf("failed to read test file data: %w", err)
		}

		// Clean up test file
		if err := driveService.Files.Delete(file.Id).Context(ctx).Do(); err != nil {
			return fmt.Errorf("failed to clean up test file: %w", err)
		}

		// Verify data matches
		if string(readData) != string(testData) {
			return fmt.Errorf(
				"test file data mismatch: expected %q, got %q",
				string(testData),
				string(readData),
			)
		}

		return nil
	})
}

// withRetryOnAuth executes the provided function with retry logic for authentication errors
func (s *GoogleDriveStorage) withRetryOnAuth(fn func(*drive.Service) error) error {
	driveService, err := s.getDriveService()
	if err != nil {
		return err
	}

	err = fn(driveService)
	if err != nil && s.isAuthError(err) {
		// Try to refresh token and retry once
		fmt.Printf("Google Drive auth error detected, attempting token refresh: %v\n", err)

		if refreshErr := s.refreshToken(); refreshErr != nil {
			// If refresh fails, return a more helpful error message
			if strings.Contains(refreshErr.Error(), "invalid_grant") ||
				strings.Contains(refreshErr.Error(), "refresh token") {
				return fmt.Errorf(
					"google drive refresh token has expired. Please re-authenticate and update your token configuration. Original error: %w. Refresh error: %v",
					err,
					refreshErr,
				)
			}

			return fmt.Errorf("failed to refresh token after auth error: %w", refreshErr)
		}

		fmt.Printf("Token refresh successful, retrying operation\n")

		// Get new service with refreshed token
		driveService, err = s.getDriveService()
		if err != nil {
			return fmt.Errorf("failed to create service after token refresh: %w", err)
		}

		// Retry the operation
		err = fn(driveService)
		if err != nil {
			fmt.Printf("Retry after token refresh also failed: %v\n", err)
		} else {
			fmt.Printf("Operation succeeded after token refresh\n")
		}
	}

	return err
}

// isAuthError checks if the error is a 401 authentication error
func (s *GoogleDriveStorage) isAuthError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "401") ||
		strings.Contains(errStr, "Invalid Credentials") ||
		strings.Contains(errStr, "authError") ||
		strings.Contains(errStr, "invalid authentication credentials")
}

// refreshToken refreshes the OAuth2 token and updates the TokenJSON field
func (s *GoogleDriveStorage) refreshToken() error {
	if err := s.Validate(); err != nil {
		return err
	}

	var token oauth2.Token
	if err := json.Unmarshal([]byte(s.TokenJSON), &token); err != nil {
		return fmt.Errorf("invalid token JSON: %w", err)
	}

	// Check if we have a refresh token
	if token.RefreshToken == "" {
		return fmt.Errorf("no refresh token available in stored token")
	}

	fmt.Printf("Original token - Access Token: %s..., Refresh Token: %s..., Expiry: %v\n",
		truncateString(token.AccessToken, 20),
		truncateString(token.RefreshToken, 20),
		token.Expiry)

	// Debug: Print the full token JSON structure (sensitive data masked)
	fmt.Printf("Original token JSON structure: %s\n", maskSensitiveData(s.TokenJSON))

	ctx := context.Background()
	cfg := &oauth2.Config{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{"https://www.googleapis.com/auth/drive.file"},
	}

	// Force the token to be expired so refresh is guaranteed
	token.Expiry = time.Now().Add(-time.Hour)
	fmt.Printf("Forcing token expiry to trigger refresh: %v\n", token.Expiry)

	tokenSource := cfg.TokenSource(ctx, &token)

	// Force token refresh
	fmt.Printf("Attempting to refresh Google Drive token...\n")
	newToken, err := tokenSource.Token()
	if err != nil {
		return fmt.Errorf("failed to refresh token: %w", err)
	}

	fmt.Printf("New token - Access Token: %s..., Refresh Token: %s..., Expiry: %v\n",
		truncateString(newToken.AccessToken, 20),
		truncateString(newToken.RefreshToken, 20),
		newToken.Expiry)

	// Check if we actually got a new token
	if newToken.AccessToken == token.AccessToken {
		return fmt.Errorf(
			"token refresh did not return a new access token - this indicates the refresh token may be invalid",
		)
	}

	// Ensure the new token has a refresh token (preserve the original if not returned)
	if newToken.RefreshToken == "" {
		fmt.Printf("New token doesn't have refresh token, preserving original\n")
		newToken.RefreshToken = token.RefreshToken
	}

	// Update the stored token JSON
	newTokenJSON, err := json.Marshal(newToken)
	if err != nil {
		return fmt.Errorf("failed to marshal refreshed token: %w", err)
	}

	s.TokenJSON = string(newTokenJSON)
	fmt.Printf("Token refresh completed successfully with new access token\n")
	return nil
}

// maskSensitiveData masks sensitive information in token JSON for logging
func maskSensitiveData(tokenJSON string) string {
	// Replace sensitive values with masked versions
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(tokenJSON), &data); err != nil {
		return "invalid JSON"
	}

	if accessToken, ok := data["access_token"].(string); ok && len(accessToken) > 10 {
		data["access_token"] = accessToken[:10] + "..."
	}
	if refreshToken, ok := data["refresh_token"].(string); ok && len(refreshToken) > 10 {
		data["refresh_token"] = refreshToken[:10] + "..."
	}

	masked, _ := json.Marshal(data)
	return string(masked)
}

// truncateString safely truncates a string for logging purposes
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func (s *GoogleDriveStorage) getDriveService() (*drive.Service, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	var token oauth2.Token
	if err := json.Unmarshal([]byte(s.TokenJSON), &token); err != nil {
		return nil, fmt.Errorf("invalid token JSON: %w", err)
	}

	ctx := context.Background()

	cfg := &oauth2.Config{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{"https://www.googleapis.com/auth/drive.file"},
	}

	tokenSource := cfg.TokenSource(ctx, &token)

	// Force token validation to ensure we're using the current token
	currentToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to get current token: %w", err)
	}

	// Create a new token source with the validated token
	validatedTokenSource := oauth2.StaticTokenSource(currentToken)

	driveService, err := drive.NewService(ctx, option.WithTokenSource(validatedTokenSource))
	if err != nil {
		return nil, fmt.Errorf("unable to create Drive client: %w", err)
	}

	return driveService, nil
}

func (s *GoogleDriveStorage) lookupFileID(
	driveService *drive.Service,
	name string,
) (string, error) {
	query := fmt.Sprintf("name = '%s' and trashed = false", escapeForQuery(name))

	results, err := driveService.Files.List().
		Q(query).
		Fields("files(id)").
		PageSize(1).
		Do()
	if err != nil {
		return "", fmt.Errorf("file lookup failed: %w", err)
	}

	if len(results.Files) == 0 {
		return "", fmt.Errorf("file %q not found in Google Drive", name)
	}

	return results.Files[0].Id, nil
}

func (s *GoogleDriveStorage) deleteByName(
	ctx context.Context,
	driveService *drive.Service,
	name string,
) error {
	query := fmt.Sprintf("name = '%s' and trashed = false", escapeForQuery(name))

	err := driveService.
		Files.
		List().
		Q(query).
		Fields("files(id)").
		Pages(ctx, func(p *drive.FileList) error {
			for _, file := range p.Files {
				if err := driveService.Files.Delete(file.Id).Context(ctx).Do(); err != nil {
					return err
				}
			}

			return nil
		})

	if err != nil {
		return fmt.Errorf("failed to delete %q: %w", name, err)
	}

	return nil
}

func escapeForQuery(s string) string {
	return strings.ReplaceAll(s, `'`, `\'`)
}
