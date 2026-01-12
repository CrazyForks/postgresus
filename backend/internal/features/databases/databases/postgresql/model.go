package postgresql

import (
	"context"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/tools"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"gorm.io/gorm"
)

type PostgresqlDatabase struct {
	ID uuid.UUID `json:"id" gorm:"primaryKey;type:uuid;default:gen_random_uuid()"`

	DatabaseID *uuid.UUID `json:"databaseId" gorm:"type:uuid;column:database_id"`

	Version tools.PostgresqlVersion `json:"version" gorm:"type:text;not null"`

	// connection data
	Host     string  `json:"host"     gorm:"type:text;not null"`
	Port     int     `json:"port"     gorm:"type:int;not null"`
	Username string  `json:"username" gorm:"type:text;not null"`
	Password string  `json:"password" gorm:"type:text;not null"`
	Database *string `json:"database" gorm:"type:text"`
	IsHttps  bool    `json:"isHttps"  gorm:"type:boolean;default:false"`

	// backup settings
	IncludeSchemas       []string `json:"includeSchemas" gorm:"-"`
	IncludeSchemasString string   `json:"-"              gorm:"column:include_schemas;type:text;not null;default:''"`
	CpuCount             int      `json:"cpuCount"       gorm:"column:cpu_count;type:int;not null;default:1"`

	// restore settings (not saved to DB)
	IsExcludeExtensions bool `json:"isExcludeExtensions" gorm:"-"`
}

func (p *PostgresqlDatabase) TableName() string {
	return "postgresql_databases"
}

func (p *PostgresqlDatabase) BeforeSave(_ *gorm.DB) error {
	if len(p.IncludeSchemas) > 0 {
		p.IncludeSchemasString = strings.Join(p.IncludeSchemas, ",")
	} else {
		p.IncludeSchemasString = ""
	}

	return nil
}

func (p *PostgresqlDatabase) AfterFind(_ *gorm.DB) error {
	if p.IncludeSchemasString != "" {
		p.IncludeSchemas = strings.Split(p.IncludeSchemasString, ",")
	} else {
		p.IncludeSchemas = []string{}
	}

	return nil
}

func (p *PostgresqlDatabase) Validate() error {
	if p.Host == "" {
		return errors.New("host is required")
	}

	if p.Port == 0 {
		return errors.New("port is required")
	}

	if p.Username == "" {
		return errors.New("username is required")
	}

	if p.Password == "" {
		return errors.New("password is required")
	}

	if p.CpuCount <= 0 {
		return errors.New("cpu count must be greater than 0")
	}

	return nil
}

func (p *PostgresqlDatabase) TestConnection(
	logger *slog.Logger,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return testSingleDatabaseConnection(logger, ctx, p, encryptor, databaseID)
}

func (p *PostgresqlDatabase) HideSensitiveData() {
	if p == nil {
		return
	}

	p.Password = ""
}

func (p *PostgresqlDatabase) Update(incoming *PostgresqlDatabase) {
	p.Version = incoming.Version
	p.Host = incoming.Host
	p.Port = incoming.Port
	p.Username = incoming.Username
	p.Database = incoming.Database
	p.IsHttps = incoming.IsHttps
	p.IncludeSchemas = incoming.IncludeSchemas
	p.CpuCount = incoming.CpuCount

	if incoming.Password != "" {
		p.Password = incoming.Password
	}
}

func (p *PostgresqlDatabase) EncryptSensitiveFields(
	databaseID uuid.UUID,
	encryptor encryption.FieldEncryptor,
) error {
	if p.Password != "" {
		encrypted, err := encryptor.Encrypt(databaseID, p.Password)
		if err != nil {
			return err
		}
		p.Password = encrypted
	}

	return nil
}

// PopulateDbData detects and sets the PostgreSQL version.
// This should be called before encrypting sensitive fields.
func (p *PostgresqlDatabase) PopulateDbData(
	logger *slog.Logger,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) error {
	return p.PopulateVersion(logger, encryptor, databaseID)
}

// PopulateVersion detects and sets the PostgreSQL version by querying the database.
func (p *PostgresqlDatabase) PopulateVersion(
	logger *slog.Logger,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) error {
	if p.Database == nil || *p.Database == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	password, err := decryptPasswordIfNeeded(p.Password, encryptor, databaseID)
	if err != nil {
		return fmt.Errorf("failed to decrypt password: %w", err)
	}

	connStr := buildConnectionStringForDB(p, *p.Database, password)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(ctx); closeErr != nil {
			logger.Error("Failed to close connection", "error", closeErr)
		}
	}()

	detectedVersion, err := detectDatabaseVersion(ctx, conn)
	if err != nil {
		return err
	}

	p.Version = detectedVersion
	return nil
}

// IsUserReadOnly checks if the database user has read-only privileges.
//
// This method performs a comprehensive security check by examining:
// - Role-level attributes (superuser, createrole, createdb, bypassrls, replication)
// - Database-level privileges (CREATE, TEMP)
// - Schema-level privileges (CREATE on any non-system schema)
// - Table-level write permissions (INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER)
// - Function-level privileges (EXECUTE on SECURITY DEFINER functions)
//
// A user is considered read-only only if they have ZERO write privileges
// across all levels. This ensures the database user follows the
// principle of least privilege for backup operations.
//
// Returns: (isReadOnly, detectedPrivileges, error)
func (p *PostgresqlDatabase) IsUserReadOnly(
	ctx context.Context,
	logger *slog.Logger,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) (bool, []string, error) {
	password, err := decryptPasswordIfNeeded(p.Password, encryptor, databaseID)
	if err != nil {
		return false, nil, fmt.Errorf("failed to decrypt password: %w", err)
	}

	connStr := buildConnectionStringForDB(p, *p.Database, password)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return false, nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(ctx); closeErr != nil {
			logger.Error("Failed to close connection", "error", closeErr)
		}
	}()

	var privileges []string

	// LEVEL 1: Check role-level attributes
	var isSuperuser, canCreateRole, canCreateDB, canBypassRLS, canReplication bool
	err = conn.QueryRow(ctx, `
		SELECT
			rolsuper,
			rolcreaterole,
			rolcreatedb,
			rolbypassrls,
			rolreplication
		FROM pg_roles
		WHERE rolname = current_user
	`).Scan(&isSuperuser, &canCreateRole, &canCreateDB, &canBypassRLS, &canReplication)
	if err != nil {
		return false, nil, fmt.Errorf("failed to check role attributes: %w", err)
	}

	if isSuperuser {
		privileges = append(privileges, "SUPERUSER")
	}
	if canCreateRole {
		privileges = append(privileges, "CREATEROLE")
	}
	if canCreateDB {
		privileges = append(privileges, "CREATEDB")
	}
	if canBypassRLS {
		privileges = append(privileges, "BYPASSRLS")
	}
	if canReplication {
		privileges = append(privileges, "REPLICATION")
	}

	// LEVEL 2: Check database-level privileges
	var canCreate, canTemp bool
	err = conn.QueryRow(ctx, `
		SELECT
			has_database_privilege(current_user, current_database(), 'CREATE') as can_create,
			has_database_privilege(current_user, current_database(), 'TEMP') as can_temp
	`).Scan(&canCreate, &canTemp)
	if err != nil {
		return false, nil, fmt.Errorf("failed to check database privileges: %w", err)
	}

	if canCreate {
		privileges = append(privileges, "CREATE (database)")
	}
	if canTemp {
		privileges = append(privileges, "TEMP")
	}

	// LEVEL 2.5: Check schema-level CREATE privileges
	var hasSchemaCreate bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM pg_namespace n
			WHERE has_schema_privilege(current_user, n.nspname, 'CREATE')
			AND nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		)
	`).Scan(&hasSchemaCreate)
	if err != nil {
		return false, nil, fmt.Errorf("failed to check schema privileges: %w", err)
	}
	if hasSchemaCreate {
		privileges = append(privileges, "CREATE (schema)")
	}

	// LEVEL 3: Check table-level write permissions
	writePrivileges := map[string]bool{
		"INSERT":     true,
		"UPDATE":     true,
		"DELETE":     true,
		"TRUNCATE":   true,
		"REFERENCES": true,
		"TRIGGER":    true,
	}

	var tablePrivileges []string
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT privilege_type
		FROM information_schema.role_table_grants
		WHERE grantee = current_user
		AND table_schema NOT IN ('pg_catalog', 'information_schema')
	`)
	if err != nil {
		return false, nil, fmt.Errorf("failed to check table privileges: %w", err)
	}

	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			rows.Close()
			return false, nil, fmt.Errorf("failed to scan privilege: %w", err)
		}
		tablePrivileges = append(tablePrivileges, privilege)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return false, nil, fmt.Errorf("error iterating privileges: %w", err)
	}

	for _, privilege := range tablePrivileges {
		if writePrivileges[privilege] {
			privileges = append(privileges, privilege)
		}
	}

	// LEVEL 4: Check for EXECUTE privilege on functions that are SECURITY DEFINER
	var funcCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
		AND p.prosecdef = true
		AND has_function_privilege(current_user, p.oid, 'EXECUTE')
	`).Scan(&funcCount)
	if err != nil {
		return false, nil, fmt.Errorf("failed to check function privileges: %w", err)
	}
	if funcCount > 0 {
		privileges = append(privileges, "EXECUTE (SECURITY DEFINER)")
	}

	isReadOnly := len(privileges) == 0
	return isReadOnly, privileges, nil
}

// CreateReadOnlyUser creates a new PostgreSQL user with read-only privileges.
//
// This method performs the following operations atomically in a single transaction:
// 1. Creates a PostgreSQL user with a UUID-based password
// 2. Grants CONNECT privilege on the database
// 3. Grants USAGE on all non-system schemas
// 4. Grants SELECT on all existing tables and sequences
// 5. Sets default privileges for future tables and sequences
//
// Security features:
// - Username format: "databasus-{8-char-uuid}" for uniqueness
// - Password: Full UUID (36 characters) for strong entropy
// - Transaction safety: All operations rollback on any failure
// - Retry logic: Up to 3 attempts if username collision occurs
// - Pre-validation: Checks CREATEROLE privilege before starting transaction
func (p *PostgresqlDatabase) CreateReadOnlyUser(
	ctx context.Context,
	logger *slog.Logger,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) (string, string, error) {
	password, err := decryptPasswordIfNeeded(p.Password, encryptor, databaseID)
	if err != nil {
		return "", "", fmt.Errorf("failed to decrypt password: %w", err)
	}

	connStr := buildConnectionStringForDB(p, *p.Database, password)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return "", "", fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(ctx); closeErr != nil {
			logger.Error("Failed to close connection", "error", closeErr)
		}
	}()

	// Pre-validate: Check if current user can create roles
	var canCreateRole, isSuperuser bool
	err = conn.QueryRow(ctx, `
		SELECT rolcreaterole, rolsuper
		FROM pg_roles
		WHERE rolname = current_user
	`).Scan(&canCreateRole, &isSuperuser)
	if err != nil {
		return "", "", fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canCreateRole && !isSuperuser {
		return "", "", errors.New("current database user lacks CREATEROLE privilege")
	}

	// Retry logic for username collision
	maxRetries := 3
	for attempt := range maxRetries {
		// Generate base username for PostgreSQL user creation
		baseUsername := fmt.Sprintf("databasus-%s", uuid.New().String()[:8])

		// For Supabase session pooler, the username format for connection is "username.projectid"
		// but the actual PostgreSQL user must be created with just the base name.
		// The pooler will strip the ".projectid" suffix when authenticating.
		connectionUsername := baseUsername
		if isSupabaseConnection(p.Host, p.Username) {
			if supabaseProjectID := extractSupabaseProjectID(p.Username); supabaseProjectID != "" {
				connectionUsername = fmt.Sprintf("%s.%s", baseUsername, supabaseProjectID)
			}
		}

		newPassword := encryption.GenerateComplexPassword()

		tx, err := conn.Begin(ctx)
		if err != nil {
			return "", "", fmt.Errorf("failed to begin transaction: %w", err)
		}

		success := false
		defer func() {
			if !success {
				if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
					logger.Error("Failed to rollback transaction", "error", rollbackErr)
				}
			}
		}()

		// Step 1: Create PostgreSQL user with LOGIN privilege
		// Note: We use baseUsername for the actual PostgreSQL user name if Supabase is used
		_, err = tx.Exec(
			ctx,
			fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s' LOGIN`, baseUsername, newPassword),
		)
		if err != nil {
			if err.Error() != "" && attempt < maxRetries-1 {
				continue
			}
			return "", "", fmt.Errorf("failed to create user: %w", err)
		}

		// Step 1.5: Revoke CREATE privilege from PUBLIC role on public schema
		// This is necessary because all PostgreSQL users inherit CREATE privilege on the
		// public schema through the PUBLIC role. This is a one-time operation that affects
		// the entire database, making it more secure by default.
		// Note: This only affects the public schema; other schemas are unaffected.
		_, err = tx.Exec(ctx, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
		if err != nil {
			logger.Error("Failed to revoke CREATE on public from PUBLIC", "error", err)
			if !strings.Contains(err.Error(), "schema \"public\" does not exist") &&
				!strings.Contains(err.Error(), "permission denied") {
				return "", "", fmt.Errorf("failed to revoke CREATE from PUBLIC: %w", err)
			}
		}

		// Now revoke from the specific user as well (belt and suspenders)
		_, err = tx.Exec(ctx, fmt.Sprintf(`REVOKE CREATE ON SCHEMA public FROM "%s"`, baseUsername))
		if err != nil {
			logger.Error(
				"Failed to revoke CREATE on public schema from user",
				"error",
				err,
				"username",
				baseUsername,
			)
		}

		// Step 2: Grant database connection privilege and revoke TEMP
		_, err = tx.Exec(
			ctx,
			fmt.Sprintf(`GRANT CONNECT ON DATABASE "%s" TO "%s"`, *p.Database, baseUsername),
		)
		if err != nil {
			return "", "", fmt.Errorf("failed to grant connect privilege: %w", err)
		}

		// Revoke TEMP privilege from PUBLIC role (like CREATE on public schema, TEMP is granted to PUBLIC by default)
		_, err = tx.Exec(ctx, fmt.Sprintf(`REVOKE TEMP ON DATABASE "%s" FROM PUBLIC`, *p.Database))
		if err != nil {
			logger.Warn("Failed to revoke TEMP from PUBLIC", "error", err)
		}

		// Also revoke from the specific user (belt and suspenders)
		_, err = tx.Exec(
			ctx,
			fmt.Sprintf(`REVOKE TEMP ON DATABASE "%s" FROM "%s"`, *p.Database, baseUsername),
		)
		if err != nil {
			logger.Warn("Failed to revoke TEMP privilege", "error", err, "username", baseUsername)
		}

		// Step 3: Discover all user-created schemas
		rows, err := tx.Query(ctx, `
			SELECT schema_name
			FROM information_schema.schemata
			WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
		`)
		if err != nil {
			return "", "", fmt.Errorf("failed to get schemas: %w", err)
		}

		var schemas []string
		for rows.Next() {
			var schema string
			if err := rows.Scan(&schema); err != nil {
				rows.Close()
				return "", "", fmt.Errorf("failed to scan schema: %w", err)
			}
			schemas = append(schemas, schema)
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return "", "", fmt.Errorf("error iterating schemas: %w", err)
		}

		// Step 4: Grant USAGE on each schema and explicitly prevent CREATE
		for _, schema := range schemas {
			// Revoke CREATE specifically (handles inheritance from PUBLIC role)
			_, err = tx.Exec(
				ctx,
				fmt.Sprintf(`REVOKE CREATE ON SCHEMA "%s" FROM "%s"`, schema, baseUsername),
			)
			if err != nil {
				logger.Warn(
					"Failed to revoke CREATE on schema",
					"error",
					err,
					"schema",
					schema,
					"username",
					baseUsername,
				)
			}

			// Grant only USAGE (not CREATE)
			_, err = tx.Exec(
				ctx,
				fmt.Sprintf(`GRANT USAGE ON SCHEMA "%s" TO "%s"`, schema, baseUsername),
			)
			if err != nil {
				return "", "", fmt.Errorf("failed to grant usage on schema %s: %w", schema, err)
			}
		}

		// Step 5: Grant SELECT on ALL existing tables and sequences
		grantSelectSQL := fmt.Sprintf(`
			DO $$
			DECLARE
				schema_rec RECORD;
			BEGIN
				FOR schema_rec IN
					SELECT schema_name
					FROM information_schema.schemata
					WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
				LOOP
					EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %%I TO "%s"', schema_rec.schema_name);
					EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %%I TO "%s"', schema_rec.schema_name);
				END LOOP;
			END $$;
		`, baseUsername, baseUsername)

		_, err = tx.Exec(ctx, grantSelectSQL)
		if err != nil {
			return "", "", fmt.Errorf("failed to grant select on tables: %w", err)
		}

		// Step 6: Set default privileges for FUTURE tables and sequences
		defaultPrivilegesSQL := fmt.Sprintf(`
			DO $$
			DECLARE
				schema_rec RECORD;
			BEGIN
				FOR schema_rec IN
					SELECT schema_name
					FROM information_schema.schemata
					WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
				LOOP
					EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %%I GRANT SELECT ON TABLES TO "%s"', schema_rec.schema_name);
					EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %%I GRANT SELECT ON SEQUENCES TO "%s"', schema_rec.schema_name);
				END LOOP;
			END $$;
		`, baseUsername, baseUsername)

		_, err = tx.Exec(ctx, defaultPrivilegesSQL)
		if err != nil {
			return "", "", fmt.Errorf("failed to set default privileges: %w", err)
		}

		// Step 7: Verify user creation before committing
		var verifyUsername string
		err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT rolname FROM pg_roles WHERE rolname = '%s'`, baseUsername)).
			Scan(&verifyUsername)
		if err != nil {
			return "", "", fmt.Errorf("failed to verify user creation: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return "", "", fmt.Errorf("failed to commit transaction: %w", err)
		}

		success = true
		// Return connectionUsername (with project ID suffix for Supabase) for the caller to use when connecting
		logger.Info(
			"Read-only user created successfully",
			"username",
			baseUsername,
			"connectionUsername",
			connectionUsername,
		)
		return connectionUsername, newPassword, nil
	}

	return "", "", errors.New("failed to generate unique username after 3 attempts")
}

// testSingleDatabaseConnection tests connection to a specific database for pg_dump
func testSingleDatabaseConnection(
	logger *slog.Logger,
	ctx context.Context,
	postgresDb *PostgresqlDatabase,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) error {
	// For single database backup, we need to connect to the specific database
	if postgresDb.Database == nil || *postgresDb.Database == "" {
		return errors.New("database name is required for single database backup (pg_dump)")
	}

	// Decrypt password if needed
	password, err := decryptPasswordIfNeeded(postgresDb.Password, encryptor, databaseID)
	if err != nil {
		return fmt.Errorf("failed to decrypt password: %w", err)
	}

	// Build connection string for the specific database
	connStr := buildConnectionStringForDB(postgresDb, *postgresDb.Database, password)

	// Test connection
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		// TODO make more readable errors:
		// - handle wrong creds
		// - handle wrong database name
		// - handle wrong protocol
		return fmt.Errorf("failed to connect to database '%s': %w", *postgresDb.Database, err)
	}
	defer func() {
		if closeErr := conn.Close(ctx); closeErr != nil {
			logger.Error("Failed to close connection", "error", closeErr)
		}
	}()

	// Detect and set the database version automatically
	detectedVersion, err := detectDatabaseVersion(ctx, conn)
	if err != nil {
		return err
	}
	postgresDb.Version = detectedVersion

	// Verify user has sufficient permissions for backup operations
	if err := checkBackupPermissions(ctx, conn, postgresDb.IncludeSchemas); err != nil {
		return err
	}

	return nil
}

// detectDatabaseVersion queries and returns the PostgreSQL major version
func detectDatabaseVersion(ctx context.Context, conn *pgx.Conn) (tools.PostgresqlVersion, error) {
	var versionStr string
	err := conn.QueryRow(ctx, "SELECT version()").Scan(&versionStr)
	if err != nil {
		return "", fmt.Errorf("failed to query database version: %w", err)
	}

	// Parse version from string like "PostgreSQL 14.2 on x86_64-pc-linux-gnu..."
	// or "PostgreSQL 16 maintained by Postgre BY..." (some builds omit minor version)
	re := regexp.MustCompile(`PostgreSQL (\d+)`)
	matches := re.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		return "", fmt.Errorf("could not parse version from: %s", versionStr)
	}

	majorVersion := matches[1]

	// Map to known PostgresqlVersion enum values
	switch majorVersion {
	case "12", "13", "14", "15", "16", "17", "18":
		return tools.PostgresqlVersion(majorVersion), nil
	default:
		return "", fmt.Errorf("unsupported PostgreSQL version: %s", majorVersion)
	}
}

// checkBackupPermissions verifies the user has sufficient privileges for pg_dump backup.
// Required privileges: CONNECT on database, USAGE on schemas, SELECT on tables.
// If includeSchemas is specified, only checks permissions on those schemas.
func checkBackupPermissions(
	ctx context.Context,
	conn *pgx.Conn,
	includeSchemas []string,
) error {
	var missingPrivileges []string

	// Check CONNECT privilege on database
	var hasConnect bool
	err := conn.QueryRow(ctx, "SELECT has_database_privilege(current_user, current_database(), 'CONNECT')").
		Scan(&hasConnect)
	if err != nil {
		return fmt.Errorf("cannot check database privileges: %w", err)
	}
	if !hasConnect {
		missingPrivileges = append(missingPrivileges, "CONNECT on database")
	}

	// Check USAGE privilege on at least one non-system schema
	var schemaCount int
	if len(includeSchemas) > 0 {
		// Check only the specified schemas
		err = conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_namespace n
			WHERE has_schema_privilege(current_user, n.nspname, 'USAGE')
			AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
			AND n.nspname NOT LIKE 'pg_temp_%'
			AND n.nspname NOT LIKE 'pg_toast_temp_%'
			AND n.nspname = ANY($1::text[])
		`, includeSchemas).Scan(&schemaCount)
	} else {
		// Check all non-system schemas
		err = conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_namespace n
			WHERE has_schema_privilege(current_user, n.nspname, 'USAGE')
			AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
			AND n.nspname NOT LIKE 'pg_temp_%'
			AND n.nspname NOT LIKE 'pg_toast_temp_%'
		`).Scan(&schemaCount)
	}

	if err != nil {
		return fmt.Errorf("cannot check schema privileges: %w", err)
	}
	if schemaCount == 0 {
		missingPrivileges = append(missingPrivileges, "USAGE on at least one schema")
	}

	// Check SELECT privilege on at least one table (if tables exist)
	// Use pg_tables from pg_catalog which shows all tables regardless of user privileges
	var tableCount int
	if len(includeSchemas) > 0 {
		// Check only tables in the specified schemas
		err = conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_catalog.pg_tables t
			WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
			AND t.schemaname = ANY($1::text[])
		`, includeSchemas).Scan(&tableCount)
	} else {
		// Check all tables in non-system schemas
		err = conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_catalog.pg_tables t
			WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
		`).Scan(&tableCount)
	}

	if err != nil {
		return fmt.Errorf("cannot check table count: %w", err)
	}

	if tableCount > 0 {
		// Check if user has SELECT on at least one of these tables
		var selectableTableCount int
		if len(includeSchemas) > 0 {
			// Check only tables in the specified schemas
			err = conn.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM pg_catalog.pg_tables t
				WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
				AND t.schemaname = ANY($1::text[])
				AND has_table_privilege(current_user, quote_ident(t.schemaname) || '.' || quote_ident(t.tablename), 'SELECT')
			`, includeSchemas).Scan(&selectableTableCount)
		} else {
			// Check all tables in non-system schemas
			err = conn.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM pg_catalog.pg_tables t
				WHERE t.schemaname NOT IN ('pg_catalog', 'information_schema')
				AND has_table_privilege(current_user, quote_ident(t.schemaname) || '.' || quote_ident(t.tablename), 'SELECT')
			`).Scan(&selectableTableCount)
		}

		if err != nil {
			return fmt.Errorf("cannot check SELECT privileges: %w", err)
		}
		if selectableTableCount == 0 {
			missingPrivileges = append(missingPrivileges, "SELECT on tables")
		}
	}

	if len(missingPrivileges) > 0 {
		return fmt.Errorf(
			"insufficient permissions for backup. Missing: %s. Required: CONNECT on database, USAGE on schemas, SELECT on tables",
			strings.Join(missingPrivileges, ", "),
		)
	}

	return nil
}

// buildConnectionStringForDB builds connection string for specific database
func buildConnectionStringForDB(p *PostgresqlDatabase, dbName string, password string) string {
	sslMode := "disable"
	if p.IsHttps {
		sslMode = "require"
	}

	return fmt.Sprintf(
		"host=%s port=%d user=%s password='%s' dbname=%s sslmode=%s default_query_exec_mode=simple_protocol standard_conforming_strings=on client_encoding=UTF8",
		p.Host,
		p.Port,
		p.Username,
		escapeConnectionStringValue(password),
		dbName,
		sslMode,
	)
}

func escapeConnectionStringValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return value
}

func decryptPasswordIfNeeded(
	password string,
	encryptor encryption.FieldEncryptor,
	databaseID uuid.UUID,
) (string, error) {
	if encryptor == nil {
		return password, nil
	}
	return encryptor.Decrypt(databaseID, password)
}

func isSupabaseConnection(host, username string) bool {
	return strings.Contains(strings.ToLower(host), "supabase") ||
		strings.Contains(strings.ToLower(username), "supabase")
}

func extractSupabaseProjectID(username string) string {
	if idx := strings.Index(username, "."); idx != -1 {
		return username[idx+1:]
	}
	return ""
}
