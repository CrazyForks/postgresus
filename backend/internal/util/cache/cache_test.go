package cache_utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ClearAllCache_AfterClear_CacheIsEmpty(t *testing.T) {
	client := getCache()

	// Arrange: Set up multiple cache entries with different prefixes
	testKeys := []struct {
		prefix string
		key    string
		value  string
	}{
		{"test:user:", "user1", "John Doe"},
		{"test:user:", "user2", "Jane Smith"},
		{"test:session:", "session1", "abc123"},
		{"test:session:", "session2", "def456"},
		{"test:data:", "item1", "value1"},
	}

	// Set all test keys
	for _, tk := range testKeys {
		cacheUtil := NewCacheUtil[string](client, tk.prefix)
		cacheUtil.Set(tk.key, &tk.value)
	}

	// Verify keys were set correctly before clearing
	for _, tk := range testKeys {
		cacheUtil := NewCacheUtil[string](client, tk.prefix)
		retrieved := cacheUtil.Get(tk.key)
		assert.NotNil(t, retrieved, "Key %s should exist before clearing", tk.prefix+tk.key)
		assert.Equal(t, tk.value, *retrieved, "Retrieved value should match set value")
	}

	// Act: Clear all cache
	err := ClearAllCache()

	// Assert: No error returned
	assert.NoError(t, err, "ClearAllCache should not return an error")

	// Assert: All keys should be deleted
	for _, tk := range testKeys {
		cacheUtil := NewCacheUtil[string](client, tk.prefix)
		retrieved := cacheUtil.Get(tk.key)
		assert.Nil(t, retrieved, "Key %s should be deleted after clearing", tk.prefix+tk.key)
	}
}
