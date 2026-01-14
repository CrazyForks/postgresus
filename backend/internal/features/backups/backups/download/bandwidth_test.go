package backups_download

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_BandwidthManager_RegisterSingleDownload(t *testing.T) {
	throughputMBs := 100
	manager := NewBandwidthManager(throughputMBs)

	expectedBytesPerSec := int64(100 * 1024 * 1024 * 75 / 100)
	assert.Equal(t, expectedBytesPerSec, manager.maxTotalBytesPerSecond)
	assert.Equal(t, expectedBytesPerSec, manager.bytesPerSecondPerDownload)

	userID := uuid.New()
	rateLimiter, err := manager.RegisterDownload(userID)
	assert.NoError(t, err)
	assert.NotNil(t, rateLimiter)

	assert.Equal(t, 1, manager.GetActiveDownloadCount())
	assert.Equal(t, expectedBytesPerSec, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedBytesPerSec, rateLimiter.bytesPerSecond)
}

func Test_BandwidthManager_RegisterMultipleDownloads_BandwidthShared(t *testing.T) {
	throughputMBs := 100
	manager := NewBandwidthManager(throughputMBs)

	maxBytes := int64(100 * 1024 * 1024 * 75 / 100)

	user1 := uuid.New()
	rateLimiter1, err := manager.RegisterDownload(user1)
	assert.NoError(t, err)
	assert.Equal(t, maxBytes, rateLimiter1.bytesPerSecond)

	user2 := uuid.New()
	rateLimiter2, err := manager.RegisterDownload(user2)
	assert.NoError(t, err)

	expectedPerDownload := maxBytes / 2
	assert.Equal(t, expectedPerDownload, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedPerDownload, rateLimiter1.bytesPerSecond)
	assert.Equal(t, expectedPerDownload, rateLimiter2.bytesPerSecond)
	assert.Equal(t, expectedPerDownload, rateLimiter2.bytesPerSecond)

	user3 := uuid.New()
	rateLimiter3, err := manager.RegisterDownload(user3)
	assert.NoError(t, err)

	expectedPerDownload = maxBytes / 3
	assert.Equal(t, expectedPerDownload, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedPerDownload, rateLimiter1.bytesPerSecond)
	assert.Equal(t, expectedPerDownload, rateLimiter2.bytesPerSecond)
	assert.Equal(t, expectedPerDownload, rateLimiter3.bytesPerSecond)
	assert.Equal(t, 3, manager.GetActiveDownloadCount())
}

func Test_BandwidthManager_UnregisterDownload_BandwidthRebalanced(t *testing.T) {
	throughputMBs := 100
	manager := NewBandwidthManager(throughputMBs)

	maxBytes := int64(100 * 1024 * 1024 * 75 / 100)

	user1 := uuid.New()
	rateLimiter1, _ := manager.RegisterDownload(user1)

	user2 := uuid.New()
	_, _ = manager.RegisterDownload(user2)

	user3 := uuid.New()
	rateLimiter3, _ := manager.RegisterDownload(user3)

	assert.Equal(t, 3, manager.GetActiveDownloadCount())
	expectedPerDownload := maxBytes / 3
	assert.Equal(t, expectedPerDownload, rateLimiter1.bytesPerSecond)

	manager.UnregisterDownload(user2)

	assert.Equal(t, 2, manager.GetActiveDownloadCount())
	expectedPerDownload = maxBytes / 2
	assert.Equal(t, expectedPerDownload, manager.bytesPerSecondPerDownload)
	assert.Equal(t, expectedPerDownload, rateLimiter1.bytesPerSecond)
	assert.Equal(t, expectedPerDownload, rateLimiter3.bytesPerSecond)

	manager.UnregisterDownload(user1)

	assert.Equal(t, 1, manager.GetActiveDownloadCount())
	assert.Equal(t, maxBytes, manager.bytesPerSecondPerDownload)
	assert.Equal(t, maxBytes, rateLimiter3.bytesPerSecond)

	manager.UnregisterDownload(user3)
	assert.Equal(t, 0, manager.GetActiveDownloadCount())
	assert.Equal(t, maxBytes, manager.bytesPerSecondPerDownload)
}

func Test_BandwidthManager_RegisterDuplicateUser_ReturnsError(t *testing.T) {
	manager := NewBandwidthManager(100)

	userID := uuid.New()
	_, err := manager.RegisterDownload(userID)
	assert.NoError(t, err)

	_, err = manager.RegisterDownload(userID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "download already registered")
}

func Test_RateLimiter_TokenBucketBasic(t *testing.T) {
	bytesPerSec := int64(1024 * 1024)
	limiter := NewRateLimiter(bytesPerSec)

	assert.Equal(t, bytesPerSec, limiter.bytesPerSecond)
	assert.Equal(t, bytesPerSec*2, limiter.bucketSize)

	start := time.Now()
	limiter.Wait(512 * 1024)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 100*time.Millisecond)
}

func Test_RateLimiter_UpdateRate(t *testing.T) {
	limiter := NewRateLimiter(1024 * 1024)

	assert.Equal(t, int64(1024*1024), limiter.bytesPerSecond)

	newRate := int64(2 * 1024 * 1024)
	limiter.UpdateRate(newRate)

	assert.Equal(t, newRate, limiter.bytesPerSecond)
	assert.Equal(t, newRate*2, limiter.bucketSize)
}

func Test_RateLimiter_ThrottlesCorrectly(t *testing.T) {
	bytesPerSec := int64(1024 * 1024)
	limiter := NewRateLimiter(bytesPerSec)

	limiter.availableTokens = 0

	start := time.Now()
	limiter.Wait(bytesPerSec / 2)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 400*time.Millisecond)
	assert.LessOrEqual(t, elapsed, 700*time.Millisecond)
}
