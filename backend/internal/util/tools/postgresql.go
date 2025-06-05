package tools

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	env_utils "postgresus-backend/internal/util/env"
	"postgresus-backend/internal/util/logger"
)

var log = logger.GetLogger()

// GetPostgresqlExecutable returns the full path to a specific PostgreSQL executable
// for the given version. Common executables include: pg_dump, psql, etc.
// On Windows, automatically appends .exe extension.
func GetPostgresqlExecutable(
	version PostgresqlVersion,
	executable PostgresqlExecutable,
	envMode env_utils.EnvMode,
	postgresesInstallDir string,
) string {
	basePath := getPostgresqlBasePath(version, envMode, postgresesInstallDir)
	executableName := string(executable)

	// Add .exe extension on Windows
	if runtime.GOOS == "windows" {
		executableName += ".exe"
	}

	return filepath.Join(basePath, executableName)
}

// VerifyPostgresesInstallation verifies that PostgreSQL versions 13-17 are installed
// in the current environment. Each version should be installed with the required
// client tools (pg_dump, psql) available.
// In development: ./tools/postgresql/postgresql-{VERSION}/bin
// In production: /usr/pgsql-{VERSION}/bin
func VerifyPostgresesInstallation(envMode env_utils.EnvMode, postgresesInstallDir string) {
	versions := []PostgresqlVersion{
		PostgresqlVersion13,
		PostgresqlVersion14,
		PostgresqlVersion15,
		PostgresqlVersion16,
		PostgresqlVersion17,
	}

	requiredCommands := []PostgresqlExecutable{
		PostgresqlExecutablePgDump,
		PostgresqlExecutablePsql,
	}

	for _, version := range versions {
		binDir := getPostgresqlBasePath(version, envMode, postgresesInstallDir)

		log.Info(
			"Verifying PostgreSQL installation",
			"version",
			string(version),
			"path",
			binDir,
		)

		if _, err := os.Stat(binDir); os.IsNotExist(err) {
			if envMode == env_utils.EnvModeDevelopment {
				log.Error(
					"PostgreSQL bin directory not found. Make sure PostgreSQL is installed. Read ./tools/readme.md for details",
					"version",
					string(version),
					"path",
					binDir,
				)
			} else {
				log.Error(
					"PostgreSQL bin directory not found. Please ensure PostgreSQL client tools are installed.",
					"version",
					string(version),
					"path",
					binDir,
				)
			}
			os.Exit(1)
		}

		for _, cmd := range requiredCommands {
			cmdPath := GetPostgresqlExecutable(
				version,
				cmd,
				envMode,
				postgresesInstallDir,
			)

			log.Info(
				"Checking for PostgreSQL command",
				"command",
				cmd,
				"version",
				string(version),
				"path",
				cmdPath,
			)

			if _, err := os.Stat(cmdPath); os.IsNotExist(err) {
				if envMode == env_utils.EnvModeDevelopment {
					log.Error(
						"PostgreSQL command not found. Make sure PostgreSQL is installed. Read ./tools/readme.md for details",
						"command",
						cmd,
						"version",
						string(version),
						"path",
						cmdPath,
					)
				} else {
					log.Error(
						"PostgreSQL command not found. Please ensure PostgreSQL client tools are properly installed.",
						"command",
						cmd,
						"version",
						string(version),
						"path",
						cmdPath,
					)
				}
				os.Exit(1)
			}

			log.Info(
				"PostgreSQL command found",
				"command",
				cmd,
				"version",
				string(version),
			)
		}

		log.Info(
			"Installation of PostgreSQL verified",
			"version",
			string(version),
			"path",
			binDir,
		)
	}

	log.Info("All PostgreSQL version-specific client tools verification completed successfully!")
}

func getPostgresqlBasePath(
	version PostgresqlVersion,
	envMode env_utils.EnvMode,
	postgresesInstallDir string,
) string {
	if envMode == env_utils.EnvModeDevelopment {
		return filepath.Join(
			postgresesInstallDir,
			fmt.Sprintf("postgresql-%s", string(version)),
			"bin",
		)
	} else {
		return fmt.Sprintf("/usr/pgsql-%s/bin", string(version))
	}
}
