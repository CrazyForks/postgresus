package backups_download

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type BandwidthManager struct {
	mu                        sync.RWMutex
	activeDownloads           map[uuid.UUID]*activeDownload
	maxTotalBytesPerSecond    int64
	bytesPerSecondPerDownload int64
}

type activeDownload struct {
	userID      uuid.UUID
	rateLimiter *RateLimiter
}

func NewBandwidthManager(throughputMBs int) *BandwidthManager {
	// Use 75% of total throughput
	maxBytes := int64(throughputMBs) * 1024 * 1024 * 75 / 100

	return &BandwidthManager{
		activeDownloads:           make(map[uuid.UUID]*activeDownload),
		maxTotalBytesPerSecond:    maxBytes,
		bytesPerSecondPerDownload: maxBytes,
	}
}

func (bm *BandwidthManager) RegisterDownload(userID uuid.UUID) (*RateLimiter, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, exists := bm.activeDownloads[userID]; exists {
		return nil, fmt.Errorf("download already registered for user %s", userID)
	}

	rateLimiter := NewRateLimiter(bm.bytesPerSecondPerDownload)

	bm.activeDownloads[userID] = &activeDownload{
		userID:      userID,
		rateLimiter: rateLimiter,
	}

	bm.recalculateRates()

	return rateLimiter, nil
}

func (bm *BandwidthManager) UnregisterDownload(userID uuid.UUID) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	delete(bm.activeDownloads, userID)
	bm.recalculateRates()
}

func (bm *BandwidthManager) GetActiveDownloadCount() int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	return len(bm.activeDownloads)
}

func (bm *BandwidthManager) recalculateRates() {
	activeCount := len(bm.activeDownloads)

	if activeCount == 0 {
		bm.bytesPerSecondPerDownload = bm.maxTotalBytesPerSecond
		return
	}

	newRate := bm.maxTotalBytesPerSecond / int64(activeCount)
	bm.bytesPerSecondPerDownload = newRate

	for _, download := range bm.activeDownloads {
		download.rateLimiter.UpdateRate(newRate)
	}
}
