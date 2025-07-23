package storages

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"postgresus-backend/internal/config"
	google_drive_storage "postgresus-backend/internal/features/storages/models/google_drive"
	local_storage "postgresus-backend/internal/features/storages/models/local"
	nas_storage "postgresus-backend/internal/features/storages/models/nas"
	s3_storage "postgresus-backend/internal/features/storages/models/s3"
	"postgresus-backend/internal/util/logger"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type S3Container struct {
	endpoint   string
	accessKey  string
	secretKey  string
	bucketName string
	region     string
}

func Test_Storage_BasicOperations(t *testing.T) {
	ctx := context.Background()

	validateEnvVariables(t)

	// Setup S3 connection to docker-compose MinIO
	s3Container, err := setupS3Container(ctx)
	require.NoError(t, err, "Failed to setup S3 container")

	// Setup test file
	testFilePath, err := setupTestFile()
	require.NoError(t, err, "Failed to setup test file")
	defer os.Remove(testFilePath)

	// Setup NAS port
	nasPort := 445
	if portStr := config.GetEnv().TestNASPort; portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			nasPort = port
		}
	}

	// Run tests
	testCases := []struct {
		name    string
		storage StorageFileSaver
	}{
		{
			name:    "LocalStorage",
			storage: &local_storage.LocalStorage{StorageID: uuid.New()},
		},
		{
			name: "S3Storage",
			storage: &s3_storage.S3Storage{
				StorageID:   uuid.New(),
				S3Bucket:    s3Container.bucketName,
				S3Region:    s3Container.region,
				S3AccessKey: s3Container.accessKey,
				S3SecretKey: s3Container.secretKey,
				S3Endpoint:  "http://" + s3Container.endpoint,
			},
		},
		{
			name: "GoogleDriveStorage",
			storage: &google_drive_storage.GoogleDriveStorage{
				StorageID:    uuid.New(),
				ClientID:     config.GetEnv().TestGoogleDriveClientID,
				ClientSecret: config.GetEnv().TestGoogleDriveClientSecret,
				TokenJSON:    config.GetEnv().TestGoogleDriveTokenJSON,
			},
		},
		{
			name: "NASStorage",
			storage: &nas_storage.NASStorage{
				StorageID: uuid.New(),
				Host:      "localhost",
				Port:      nasPort,
				Share:     "backups",
				Username:  "testuser",
				Password:  "testpassword",
				UseSSL:    false,
				Domain:    "",
				Path:      "test-files",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("Test_TestConnection_ConnectionSucceeds", func(t *testing.T) {
				err := tc.storage.TestConnection()
				assert.NoError(t, err, "TestConnection should succeed")
			})

			t.Run("Test_TestValidation_ValidationSucceeds", func(t *testing.T) {
				err := tc.storage.Validate()
				assert.NoError(t, err, "Validate should succeed")
			})

			t.Run("Test_TestSaveAndGetFile_ReturnsCorrectContent", func(t *testing.T) {
				fileData, err := os.ReadFile(testFilePath)
				require.NoError(t, err, "Should be able to read test file")

				fileID := uuid.New()

				err = tc.storage.SaveFile(logger.GetLogger(), fileID, bytes.NewReader(fileData))
				require.NoError(t, err, "SaveFile should succeed")

				file, err := tc.storage.GetFile(fileID)
				assert.NoError(t, err, "GetFile should succeed")
				defer file.Close()

				content, err := io.ReadAll(file)
				assert.NoError(t, err, "Should be able to read file")
				assert.Equal(t, fileData, content, "File content should match the original")
			})

			t.Run("Test_TestDeleteFile_RemovesFileFromDisk", func(t *testing.T) {
				fileData, err := os.ReadFile(testFilePath)
				require.NoError(t, err, "Should be able to read test file")

				fileID := uuid.New()
				err = tc.storage.SaveFile(logger.GetLogger(), fileID, bytes.NewReader(fileData))
				require.NoError(t, err, "SaveFile should succeed")

				err = tc.storage.DeleteFile(fileID)
				assert.NoError(t, err, "DeleteFile should succeed")

				file, err := tc.storage.GetFile(fileID)
				assert.Error(t, err, "GetFile should fail for non-existent file")
				if file != nil {
					file.Close()
				}
			})

			t.Run("Test_TestDeleteNonExistentFile_DoesNotError", func(t *testing.T) {
				// Try to delete a non-existent file
				nonExistentID := uuid.New()
				err := tc.storage.DeleteFile(nonExistentID)
				assert.NoError(t, err, "DeleteFile should not error for non-existent file")
			})
		})
	}
}

func setupTestFile() (string, error) {
	tempDir := os.TempDir()
	testFilePath := filepath.Join(tempDir, "test_file.txt")
	testData := []byte("This is test data for storage testing")

	// 0644 means: owner can read/write
	err := os.WriteFile(testFilePath, testData, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to create test file: %w", err)
	}

	return testFilePath, nil
}

// setupS3Container connects to the docker-compose MinIO service
func setupS3Container(ctx context.Context) (*S3Container, error) {
	env := config.GetEnv()

	accessKey := "testuser"
	secretKey := "testpassword"
	bucketName := "test-bucket"
	region := "us-east-1"
	endpoint := fmt.Sprintf("localhost:%s", env.TestMinioPort)

	// Create MinIO client and ensure bucket exists
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	// Create the bucket
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		if err := minioClient.MakeBucket(
			ctx,
			bucketName,
			minio.MakeBucketOptions{Region: region},
		); err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return &S3Container{
		endpoint:   endpoint,
		accessKey:  accessKey,
		secretKey:  secretKey,
		bucketName: bucketName,
		region:     region,
	}, nil
}

func validateEnvVariables(t *testing.T) {
	env := config.GetEnv()
	assert.NotEmpty(t, env.TestGoogleDriveClientID, "TEST_GOOGLE_DRIVE_CLIENT_ID is empty")
	assert.NotEmpty(t, env.TestGoogleDriveClientSecret, "TEST_GOOGLE_DRIVE_CLIENT_SECRET is empty")
	assert.NotEmpty(t, env.TestGoogleDriveTokenJSON, "TEST_GOOGLE_DRIVE_TOKEN_JSON is empty")
	assert.NotEmpty(t, env.TestMinioPort, "TEST_MINIO_PORT is empty")
	assert.NotEmpty(t, env.TestNASPort, "TEST_NAS_PORT is empty")
}
