package files_utils

import (
	"fmt"
	"os"
)

func EnsureDirectories(directories []string) error {
	const directoryPermissions = 0755

	for _, directory := range directories {
		if _, err := os.Stat(directory); os.IsNotExist(err) {
			if err := os.MkdirAll(directory, directoryPermissions); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", directory, err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to check directory %s: %w", directory, err)
		}
	}

	return nil
}
