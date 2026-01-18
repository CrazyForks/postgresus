package postgresql

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"databasus-backend/internal/config"
	"databasus-backend/internal/util/tools"
)

func Test_TestConnection_PasswordContainingSpaces_TestedSuccessfully(t *testing.T) {
	env := config.GetEnv()
	container := connectToPostgresContainer(t, env.TestPostgres16Port)
	defer container.DB.Close()

	passwordWithSpaces := "test password with spaces"
	usernameWithSpaces := fmt.Sprintf("testuser_spaces_%s", uuid.New().String()[:8])

	_, err := container.DB.Exec(`
		DROP TABLE IF EXISTS password_test CASCADE;
		CREATE TABLE password_test (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		);
		INSERT INTO password_test (data) VALUES ('test1');
	`)
	assert.NoError(t, err)

	_, err = container.DB.Exec(fmt.Sprintf(
		`CREATE USER "%s" WITH PASSWORD '%s' LOGIN`,
		usernameWithSpaces,
		passwordWithSpaces,
	))
	assert.NoError(t, err)

	_, err = container.DB.Exec(fmt.Sprintf(
		`GRANT CONNECT ON DATABASE "%s" TO "%s"`,
		container.Database,
		usernameWithSpaces,
	))
	assert.NoError(t, err)

	_, err = container.DB.Exec(fmt.Sprintf(
		`GRANT USAGE ON SCHEMA public TO "%s"`,
		usernameWithSpaces,
	))
	assert.NoError(t, err)

	_, err = container.DB.Exec(fmt.Sprintf(
		`GRANT SELECT ON ALL TABLES IN SCHEMA public TO "%s"`,
		usernameWithSpaces,
	))
	assert.NoError(t, err)

	defer func() {
		_, _ = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, usernameWithSpaces))
	}()

	pgModel := &PostgresqlDatabase{
		Version:  tools.GetPostgresqlVersionEnum("16"),
		Host:     container.Host,
		Port:     container.Port,
		Username: usernameWithSpaces,
		Password: passwordWithSpaces,
		Database: &container.Database,
		IsHttps:  false,
		CpuCount: 1,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	err = pgModel.TestConnection(logger, nil, uuid.New())
	assert.NoError(t, err)
}

func Test_TestConnection_InsufficientPermissions_ReturnsError(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			_, err := container.DB.Exec(`
				DROP TABLE IF EXISTS permission_test CASCADE;
				CREATE TABLE permission_test (
					id SERIAL PRIMARY KEY,
					data TEXT NOT NULL
				);
				INSERT INTO permission_test (data) VALUES ('test1');
			`)
			assert.NoError(t, err)

			limitedUsername := fmt.Sprintf("limited_user_%s", uuid.New().String()[:8])
			limitedPassword := "limitedpassword123"

			_, err = container.DB.Exec(fmt.Sprintf(
				`CREATE USER "%s" WITH PASSWORD '%s' LOGIN`,
				limitedUsername,
				limitedPassword,
			))
			assert.NoError(t, err)

			_, err = container.DB.Exec(fmt.Sprintf(
				`GRANT CONNECT ON DATABASE "%s" TO "%s"`,
				container.Database,
				limitedUsername,
			))
			assert.NoError(t, err)

			defer func() {
				_, _ = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, limitedUsername))
			}()

			pgModel := &PostgresqlDatabase{
				Version:  tools.GetPostgresqlVersionEnum(tc.version),
				Host:     container.Host,
				Port:     container.Port,
				Username: limitedUsername,
				Password: limitedPassword,
				Database: &container.Database,
				IsHttps:  false,
				CpuCount: 1,
			}

			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

			err = pgModel.TestConnection(logger, nil, uuid.New())
			assert.Error(t, err)
			if err != nil {
				assert.Contains(t, err.Error(), "insufficient permissions")
			}
		})
	}
}

func Test_TestConnection_SufficientPermissions_Success(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			_, err := container.DB.Exec(`
				DROP TABLE IF EXISTS backup_test CASCADE;
				CREATE TABLE backup_test (
					id SERIAL PRIMARY KEY,
					data TEXT NOT NULL
				);
				INSERT INTO backup_test (data) VALUES ('test1');
			`)
			assert.NoError(t, err)

			backupUsername := fmt.Sprintf("backup_user_%s", uuid.New().String()[:8])
			backupPassword := "backuppassword123"

			_, err = container.DB.Exec(fmt.Sprintf(
				`CREATE USER "%s" WITH PASSWORD '%s' LOGIN`,
				backupUsername,
				backupPassword,
			))
			assert.NoError(t, err)

			_, err = container.DB.Exec(fmt.Sprintf(
				`GRANT CONNECT ON DATABASE "%s" TO "%s"`,
				container.Database,
				backupUsername,
			))
			assert.NoError(t, err)

			_, err = container.DB.Exec(fmt.Sprintf(
				`GRANT USAGE ON SCHEMA public TO "%s"`,
				backupUsername,
			))
			assert.NoError(t, err)

			_, err = container.DB.Exec(fmt.Sprintf(
				`GRANT SELECT ON ALL TABLES IN SCHEMA public TO "%s"`,
				backupUsername,
			))
			assert.NoError(t, err)

			defer func() {
				_, _ = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, backupUsername))
			}()

			pgModel := &PostgresqlDatabase{
				Version:  tools.GetPostgresqlVersionEnum(tc.version),
				Host:     container.Host,
				Port:     container.Port,
				Username: backupUsername,
				Password: backupPassword,
				Database: &container.Database,
				IsHttps:  false,
				CpuCount: 1,
			}

			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

			err = pgModel.TestConnection(logger, nil, uuid.New())
			assert.NoError(t, err)
		})
	}
}

func Test_IsUserReadOnly_AdminUser_ReturnsFalse(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			pgModel := createPostgresModel(container)
			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			ctx := context.Background()

			isReadOnly, privileges, err := pgModel.IsUserReadOnly(ctx, logger, nil, uuid.New())
			assert.NoError(t, err)
			assert.False(t, isReadOnly, "Admin user should not be read-only")
			assert.NotEmpty(t, privileges, "Admin user should have privileges")
		})
	}
}

func Test_IsUserReadOnly_ReadOnlyUser_ReturnsTrue(t *testing.T) {
	env := config.GetEnv()
	container := connectToPostgresContainer(t, env.TestPostgres16Port)
	defer container.DB.Close()

	_, err := container.DB.Exec(`
		DROP TABLE IF EXISTS readonly_check_test CASCADE;
		CREATE TABLE readonly_check_test (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		);
		INSERT INTO readonly_check_test (data) VALUES ('test1');
	`)
	assert.NoError(t, err)

	pgModel := createPostgresModel(container)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
	assert.NoError(t, err)

	readOnlyModel := &PostgresqlDatabase{
		Version:  pgModel.Version,
		Host:     pgModel.Host,
		Port:     pgModel.Port,
		Username: username,
		Password: password,
		Database: pgModel.Database,
		IsHttps:  false,
		CpuCount: 1,
	}

	isReadOnly, privileges, err := readOnlyModel.IsUserReadOnly(ctx, logger, nil, uuid.New())
	assert.NoError(t, err)
	assert.True(t, isReadOnly, "Read-only user should be read-only")
	assert.Empty(t, privileges, "Read-only user should have no write privileges")

	_, err = container.DB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
	if err != nil {
		t.Logf("Warning: Failed to drop owned objects: %v", err)
	}
	_, err = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
	assert.NoError(t, err)
}

func Test_CreateReadOnlyUser_UserCanReadButNotWrite(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			_, err := container.DB.Exec(`
			DROP TABLE IF EXISTS readonly_test CASCADE;
			DROP TABLE IF EXISTS hack_table CASCADE;
			DROP TABLE IF EXISTS future_table CASCADE;
			CREATE TABLE readonly_test (
				id SERIAL PRIMARY KEY,
				data TEXT NOT NULL
			);
			INSERT INTO readonly_test (data) VALUES ('test1'), ('test2');
		`)
			assert.NoError(t, err)

			pgModel := createPostgresModel(container)
			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			ctx := context.Background()

			username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
			assert.NoError(t, err)
			assert.NotEmpty(t, username)
			assert.NotEmpty(t, password)
			assert.True(t, strings.HasPrefix(username, "databasus-"))

			readOnlyModel := &PostgresqlDatabase{
				Version:  pgModel.Version,
				Host:     pgModel.Host,
				Port:     pgModel.Port,
				Username: username,
				Password: password,
				Database: pgModel.Database,
				IsHttps:  false,
			}

			isReadOnly, privileges, err := readOnlyModel.IsUserReadOnly(
				ctx,
				logger,
				nil,
				uuid.New(),
			)
			assert.NoError(t, err)
			assert.True(t, isReadOnly, "Created user should be read-only")
			assert.Empty(t, privileges, "Read-only user should have no write privileges")

			readOnlyDSN := fmt.Sprintf(
				"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				container.Host,
				container.Port,
				username,
				password,
				container.Database,
			)
			readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
			assert.NoError(t, err)
			defer readOnlyConn.Close()

			var count int
			err = readOnlyConn.Get(&count, "SELECT COUNT(*) FROM readonly_test")
			assert.NoError(t, err)
			assert.Equal(t, 2, count)

			_, err = readOnlyConn.Exec("INSERT INTO readonly_test (data) VALUES ('should-fail')")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = readOnlyConn.Exec("UPDATE readonly_test SET data = 'hacked' WHERE id = 1")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = readOnlyConn.Exec("DELETE FROM readonly_test WHERE id = 1")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = readOnlyConn.Exec("CREATE TABLE hack_table (id INT)")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = container.DB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
			if err != nil {
				t.Logf("Warning: Failed to drop owned objects: %v", err)
			}

			_, err = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
			assert.NoError(t, err)
		})
	}
}

func Test_ReadOnlyUser_FutureTables_HaveSelectPermission(t *testing.T) {
	env := config.GetEnv()
	container := connectToPostgresContainer(t, env.TestPostgres16Port)
	defer container.DB.Close()

	pgModel := createPostgresModel(container)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
	assert.NoError(t, err)

	_, err = container.DB.Exec(`
		CREATE TABLE future_table (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		);
		INSERT INTO future_table (data) VALUES ('future_data');
	`)
	assert.NoError(t, err)

	readOnlyDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		container.Host, container.Port, username, password, container.Database)
	readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
	assert.NoError(t, err)
	defer readOnlyConn.Close()

	var data string
	err = readOnlyConn.Get(&data, "SELECT data FROM future_table LIMIT 1")
	assert.NoError(t, err)
	assert.Equal(t, "future_data", data)

	_, err = container.DB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
	if err != nil {
		t.Logf("Warning: Failed to drop owned objects: %v", err)
	}

	_, err = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
	assert.NoError(t, err)
}

func Test_ReadOnlyUser_MultipleSchemas_AllAccessible(t *testing.T) {
	env := config.GetEnv()
	container := connectToPostgresContainer(t, env.TestPostgres16Port)
	defer container.DB.Close()

	_, err := container.DB.Exec(`
		DROP SCHEMA IF EXISTS schema_a CASCADE;
		DROP SCHEMA IF EXISTS schema_b CASCADE;
		CREATE SCHEMA schema_a;
		CREATE SCHEMA schema_b;
		CREATE TABLE schema_a.table_a (id INT, data TEXT);
		CREATE TABLE schema_b.table_b (id INT, data TEXT);
		INSERT INTO schema_a.table_a VALUES (1, 'data_a');
		INSERT INTO schema_b.table_b VALUES (2, 'data_b');
	`)
	assert.NoError(t, err)

	pgModel := createPostgresModel(container)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
	assert.NoError(t, err)

	readOnlyDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		container.Host, container.Port, username, password, container.Database)
	readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
	assert.NoError(t, err)
	defer readOnlyConn.Close()

	var dataA string
	err = readOnlyConn.Get(&dataA, "SELECT data FROM schema_a.table_a LIMIT 1")
	assert.NoError(t, err)
	assert.Equal(t, "data_a", dataA)

	var dataB string
	err = readOnlyConn.Get(&dataB, "SELECT data FROM schema_b.table_b LIMIT 1")
	assert.NoError(t, err)
	assert.Equal(t, "data_b", dataB)

	_, err = container.DB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
	if err != nil {
		t.Logf("Warning: Failed to drop owned objects: %v", err)
	}

	_, err = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
	assert.NoError(t, err)
	_, err = container.DB.Exec(`DROP SCHEMA schema_a CASCADE; DROP SCHEMA schema_b CASCADE;`)
	assert.NoError(t, err)
}

func Test_CreateReadOnlyUser_DatabaseNameWithDash_Success(t *testing.T) {
	env := config.GetEnv()
	container := connectToPostgresContainer(t, env.TestPostgres16Port)
	defer container.DB.Close()

	dashDbName := "test-db-with-dash"

	_, err := container.DB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dashDbName))
	assert.NoError(t, err)

	_, err = container.DB.Exec(fmt.Sprintf(`CREATE DATABASE "%s"`, dashDbName))
	assert.NoError(t, err)

	defer func() {
		_, _ = container.DB.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dashDbName))
	}()

	dashDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		container.Host, container.Port, container.Username, container.Password, dashDbName)
	dashDB, err := sqlx.Connect("postgres", dashDSN)
	assert.NoError(t, err)
	defer dashDB.Close()

	_, err = dashDB.Exec(`
		CREATE TABLE dash_test (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		);
		INSERT INTO dash_test (data) VALUES ('test1'), ('test2');
	`)
	assert.NoError(t, err)

	pgModel := &PostgresqlDatabase{
		Version:  tools.GetPostgresqlVersionEnum("16"),
		Host:     container.Host,
		Port:     container.Port,
		Username: container.Username,
		Password: container.Password,
		Database: &dashDbName,
		IsHttps:  false,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
	assert.NoError(t, err)
	assert.NotEmpty(t, username)
	assert.NotEmpty(t, password)
	assert.True(t, strings.HasPrefix(username, "databasus-"))

	readOnlyDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		container.Host, container.Port, username, password, dashDbName)
	readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
	assert.NoError(t, err)
	defer readOnlyConn.Close()

	var count int
	err = readOnlyConn.Get(&count, "SELECT COUNT(*) FROM dash_test")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	_, err = readOnlyConn.Exec("INSERT INTO dash_test (data) VALUES ('should-fail')")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")

	_, err = dashDB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
	if err != nil {
		t.Logf("Warning: Failed to drop owned objects: %v", err)
	}

	_, err = dashDB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
	assert.NoError(t, err)
}

func Test_CreateReadOnlyUser_Supabase_UserCanReadButNotWrite(t *testing.T) {
	if config.GetEnv().IsSkipExternalResourcesTests {
		t.Skip("Skipping Supabase test: IS_SKIP_EXTERNAL_RESOURCES_TESTS is true")
	}

	env := config.GetEnv()

	if env.TestSupabaseHost == "" {
		t.Skip("Skipping Supabase test: missing environment variables")
	}

	portInt, err := strconv.Atoi(env.TestSupabasePort)
	assert.NoError(t, err)

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		env.TestSupabaseHost,
		portInt,
		env.TestSupabaseUsername,
		env.TestSupabasePassword,
		env.TestSupabaseDatabase,
	)

	adminDB, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	defer adminDB.Close()

	tableName := fmt.Sprintf(
		"readonly_test_%s",
		strings.ReplaceAll(uuid.New().String()[:8], "-", ""),
	)
	_, err = adminDB.Exec(fmt.Sprintf(`
		DROP TABLE IF EXISTS public.%s CASCADE;
		CREATE TABLE public.%s (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		);
		INSERT INTO public.%s (data) VALUES ('test1'), ('test2');
	`, tableName, tableName, tableName))
	assert.NoError(t, err)

	defer func() {
		_, _ = adminDB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS public.%s CASCADE`, tableName))
	}()

	pgModel := &PostgresqlDatabase{
		Host:     env.TestSupabaseHost,
		Port:     portInt,
		Username: env.TestSupabaseUsername,
		Password: env.TestSupabasePassword,
		Database: &env.TestSupabaseDatabase,
		IsHttps:  true,
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	connectionUsername, newPassword, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
	assert.NoError(t, err)
	assert.NotEmpty(t, connectionUsername)
	assert.NotEmpty(t, newPassword)
	assert.True(t, strings.HasPrefix(connectionUsername, "databasus-"))

	baseUsername := connectionUsername
	if idx := strings.Index(connectionUsername, "."); idx != -1 {
		baseUsername = connectionUsername[:idx]
	}

	defer func() {
		_, _ = adminDB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, baseUsername))
		_, _ = adminDB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, baseUsername))
	}()

	readOnlyDSN := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		env.TestSupabaseHost,
		portInt,
		connectionUsername,
		newPassword,
		env.TestSupabaseDatabase,
	)
	readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
	assert.NoError(t, err)
	defer readOnlyConn.Close()

	var count int
	err = readOnlyConn.Get(&count, fmt.Sprintf("SELECT COUNT(*) FROM public.%s", tableName))
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	_, err = readOnlyConn.Exec(
		fmt.Sprintf("INSERT INTO public.%s (data) VALUES ('should-fail')", tableName),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")

	_, err = readOnlyConn.Exec(
		fmt.Sprintf("UPDATE public.%s SET data = 'hacked' WHERE id = 1", tableName),
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")

	_, err = readOnlyConn.Exec(fmt.Sprintf("DELETE FROM public.%s WHERE id = 1", tableName))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")

	_, err = readOnlyConn.Exec("CREATE TABLE public.hack_table (id INT)")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
}

func Test_CreateReadOnlyUser_WithPublicSchema_Success(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			_, err := container.DB.Exec(`
				DROP TABLE IF EXISTS public_schema_test CASCADE;
				CREATE TABLE public_schema_test (
					id SERIAL PRIMARY KEY,
					data TEXT NOT NULL
				);
				INSERT INTO public_schema_test (data) VALUES ('test1'), ('test2');
			`)
			assert.NoError(t, err)

			pgModel := createPostgresModel(container)
			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			ctx := context.Background()

			username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
			assert.NoError(t, err)
			assert.NotEmpty(t, username)
			assert.NotEmpty(t, password)
			assert.True(t, strings.HasPrefix(username, "databasus-"))

			readOnlyModel := &PostgresqlDatabase{
				Version:  pgModel.Version,
				Host:     pgModel.Host,
				Port:     pgModel.Port,
				Username: username,
				Password: password,
				Database: pgModel.Database,
				IsHttps:  false,
			}

			isReadOnly, privileges, err := readOnlyModel.IsUserReadOnly(
				ctx,
				logger,
				nil,
				uuid.New(),
			)
			assert.NoError(t, err)
			assert.True(t, isReadOnly, "User should be read-only")
			assert.Empty(t, privileges, "Read-only user should have no write privileges")

			readOnlyDSN := fmt.Sprintf(
				"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				container.Host,
				container.Port,
				username,
				password,
				container.Database,
			)
			readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
			assert.NoError(t, err)
			defer readOnlyConn.Close()

			var count int
			err = readOnlyConn.Get(&count, "SELECT COUNT(*) FROM public_schema_test")
			assert.NoError(t, err)
			assert.Equal(t, 2, count)

			_, err = readOnlyConn.Exec(
				"INSERT INTO public_schema_test (data) VALUES ('should-fail')",
			)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = readOnlyConn.Exec("CREATE TABLE public.hack_table (id INT)")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = container.DB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
			if err != nil {
				t.Logf("Warning: Failed to drop owned objects: %v", err)
			}
			_, err = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
			assert.NoError(t, err)
		})
	}
}

func Test_CreateReadOnlyUser_WithoutPublicSchema_Success(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			_, err := container.DB.Exec(`
				DROP SCHEMA IF EXISTS public CASCADE;
				DROP SCHEMA IF EXISTS app_schema CASCADE;
				DROP SCHEMA IF EXISTS data_schema CASCADE;
				CREATE SCHEMA app_schema;
				CREATE SCHEMA data_schema;
				CREATE TABLE app_schema.users (
					id SERIAL PRIMARY KEY,
					username TEXT NOT NULL
				);
				CREATE TABLE data_schema.records (
					id SERIAL PRIMARY KEY,
					info TEXT NOT NULL
				);
				INSERT INTO app_schema.users (username) VALUES ('user1'), ('user2');
				INSERT INTO data_schema.records (info) VALUES ('record1'), ('record2');
			`)
			assert.NoError(t, err)

			pgModel := createPostgresModel(container)
			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			ctx := context.Background()

			username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
			assert.NoError(t, err, "CreateReadOnlyUser should succeed without public schema")
			assert.NotEmpty(t, username)
			assert.NotEmpty(t, password)
			assert.True(t, strings.HasPrefix(username, "databasus-"))

			readOnlyModel := &PostgresqlDatabase{
				Version:  pgModel.Version,
				Host:     pgModel.Host,
				Port:     pgModel.Port,
				Username: username,
				Password: password,
				Database: pgModel.Database,
				IsHttps:  false,
			}

			isReadOnly, privileges, err := readOnlyModel.IsUserReadOnly(
				ctx,
				logger,
				nil,
				uuid.New(),
			)
			assert.NoError(t, err)
			assert.True(t, isReadOnly, "User should be read-only")
			assert.Empty(t, privileges, "Read-only user should have no write privileges")

			readOnlyDSN := fmt.Sprintf(
				"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				container.Host,
				container.Port,
				username,
				password,
				container.Database,
			)
			readOnlyConn, err := sqlx.Connect("postgres", readOnlyDSN)
			assert.NoError(t, err)
			defer readOnlyConn.Close()

			var userCount int
			err = readOnlyConn.Get(&userCount, "SELECT COUNT(*) FROM app_schema.users")
			assert.NoError(t, err)
			assert.Equal(t, 2, userCount)

			var recordCount int
			err = readOnlyConn.Get(&recordCount, "SELECT COUNT(*) FROM data_schema.records")
			assert.NoError(t, err)
			assert.Equal(t, 2, recordCount)

			_, err = readOnlyConn.Exec(
				"INSERT INTO app_schema.users (username) VALUES ('should-fail')",
			)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = readOnlyConn.Exec("CREATE TABLE app_schema.hack_table (id INT)")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = readOnlyConn.Exec("CREATE TABLE data_schema.hack_table (id INT)")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "permission denied")

			_, err = container.DB.Exec(fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, username))
			if err != nil {
				t.Logf("Warning: Failed to drop owned objects: %v", err)
			}
			_, err = container.DB.Exec(fmt.Sprintf(`DROP USER IF EXISTS "%s"`, username))
			assert.NoError(t, err)

			_, err = container.DB.Exec(`
				DROP SCHEMA IF EXISTS app_schema CASCADE;
				DROP SCHEMA IF EXISTS data_schema CASCADE;
				CREATE SCHEMA IF NOT EXISTS public;
			`)
			assert.NoError(t, err)
		})
	}
}

func Test_CreateReadOnlyUser_PublicSchemaExistsButNoPermissions_ReturnsError(t *testing.T) {
	env := config.GetEnv()
	cases := []struct {
		name    string
		version string
		port    string
	}{
		{"PostgreSQL 12", "12", env.TestPostgres12Port},
		{"PostgreSQL 13", "13", env.TestPostgres13Port},
		{"PostgreSQL 14", "14", env.TestPostgres14Port},
		{"PostgreSQL 15", "15", env.TestPostgres15Port},
		{"PostgreSQL 16", "16", env.TestPostgres16Port},
		{"PostgreSQL 17", "17", env.TestPostgres17Port},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			container := connectToPostgresContainer(t, tc.port)
			defer container.DB.Close()

			limitedAdminUsername := fmt.Sprintf("limited_admin_%s", uuid.New().String()[:8])
			limitedAdminPassword := "limited_password_123"

			_, err := container.DB.Exec(`
				CREATE SCHEMA IF NOT EXISTS public;
				DROP TABLE IF EXISTS public.permission_test_table CASCADE;
				CREATE TABLE public.permission_test_table (
					id SERIAL PRIMARY KEY,
					data TEXT NOT NULL
				);
				INSERT INTO public.permission_test_table (data) VALUES ('test1');
			`)
			assert.NoError(t, err)

			_, err = container.DB.Exec(`GRANT CREATE ON SCHEMA public TO PUBLIC`)
			assert.NoError(t, err)

			_, err = container.DB.Exec(fmt.Sprintf(
				`CREATE USER "%s" WITH PASSWORD '%s' LOGIN CREATEROLE`,
				limitedAdminUsername,
				limitedAdminPassword,
			))
			assert.NoError(t, err)

			_, err = container.DB.Exec(fmt.Sprintf(
				`GRANT CONNECT ON DATABASE "%s" TO "%s"`,
				container.Database,
				limitedAdminUsername,
			))
			assert.NoError(t, err)

			defer func() {
				_, _ = container.DB.Exec(
					fmt.Sprintf(`DROP OWNED BY "%s" CASCADE`, limitedAdminUsername),
				)
				_, _ = container.DB.Exec(
					fmt.Sprintf(`DROP USER IF EXISTS "%s"`, limitedAdminUsername),
				)
				_, _ = container.DB.Exec(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
			}()

			limitedAdminDSN := fmt.Sprintf(
				"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				container.Host,
				container.Port,
				limitedAdminUsername,
				limitedAdminPassword,
				container.Database,
			)
			limitedAdminConn, err := sqlx.Connect("postgres", limitedAdminDSN)
			assert.NoError(t, err)
			defer limitedAdminConn.Close()

			pgModel := &PostgresqlDatabase{
				Version:  tools.GetPostgresqlVersionEnum(tc.version),
				Host:     container.Host,
				Port:     container.Port,
				Username: limitedAdminUsername,
				Password: limitedAdminPassword,
				Database: &container.Database,
				IsHttps:  false,
			}

			logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
			ctx := context.Background()

			username, password, err := pgModel.CreateReadOnlyUser(ctx, logger, nil, uuid.New())
			assert.Error(
				t,
				err,
				"CreateReadOnlyUser should fail when admin lacks permissions to secure public schema",
			)
			if err != nil {
				errorMsg := err.Error()
				hasExpectedError := strings.Contains(
					errorMsg,
					"failed to revoke CREATE from PUBLIC on existing public schema",
				) ||
					strings.Contains(errorMsg, "permission denied for schema public") ||
					strings.Contains(errorMsg, "failed to grant")
				assert.True(
					t,
					hasExpectedError,
					"Error should indicate permission issues with public schema, got: %s",
					errorMsg,
				)
			}
			assert.Empty(t, username)
			assert.Empty(t, password)
		})
	}
}

func Test_Validate_WhenLocalhostAndDatabasus_ReturnsError(t *testing.T) {
	testCases := []struct {
		name     string
		host     string
		username string
		database string
	}{
		{
			name:     "localhost with databasus db",
			host:     "localhost",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "127.0.0.1 with databasus db",
			host:     "127.0.0.1",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "172.17.0.1 with databasus db",
			host:     "172.17.0.1",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "host.docker.internal with databasus db",
			host:     "host.docker.internal",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "LOCALHOST (uppercase) with DATABASUS db",
			host:     "LOCALHOST",
			username: "POSTGRES",
			database: "DATABASUS",
		},
		{
			name:     "LocalHost (mixed case) with DataBasus db",
			host:     "LocalHost",
			username: "anyuser",
			database: "DataBasus",
		},
		{
			name:     "localhost with databasus and any username",
			host:     "localhost",
			username: "myuser",
			database: "databasus",
		},
		{
			name:     "::1 (IPv6 loopback) with databasus db",
			host:     "::1",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     ":: (IPv6 all interfaces) with databasus db",
			host:     "::",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "::1 (uppercase) with DATABASUS db",
			host:     "::1",
			username: "POSTGRES",
			database: "DATABASUS",
		},
		{
			name:     "0.0.0.0 (all IPv4 interfaces) with databasus db",
			host:     "0.0.0.0",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "127.0.0.2 (loopback range) with databasus db",
			host:     "127.0.0.2",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "127.255.255.255 (end of loopback range) with databasus db",
			host:     "127.255.255.255",
			username: "postgres",
			database: "databasus",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pgModel := &PostgresqlDatabase{
				Host:     tc.host,
				Port:     5437,
				Username: tc.username,
				Password: "somepassword",
				Database: &tc.database,
				CpuCount: 1,
			}

			err := pgModel.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "backing up Databasus internal database is not allowed")
			assert.Contains(t, err.Error(), "https://databasus.com/faq#backup-databasus")
		})
	}
}

func Test_Validate_WhenNotLocalhostOrNotDatabasus_ValidatesSuccessfully(t *testing.T) {
	testCases := []struct {
		name     string
		host     string
		username string
		database string
	}{
		{
			name:     "different host (remote server) with databasus db",
			host:     "192.168.1.100",
			username: "postgres",
			database: "databasus",
		},
		{
			name:     "different database name on localhost",
			host:     "localhost",
			username: "postgres",
			database: "myapp",
		},
		{
			name:     "all different",
			host:     "db.example.com",
			username: "appuser",
			database: "production",
		},
		{
			name:     "localhost with postgres database",
			host:     "localhost",
			username: "postgres",
			database: "postgres",
		},
		{
			name:     "remote host with databasus db name (allowed)",
			host:     "db.example.com",
			username: "postgres",
			database: "databasus",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pgModel := &PostgresqlDatabase{
				Host:     tc.host,
				Port:     5432,
				Username: tc.username,
				Password: "somepassword",
				Database: &tc.database,
				CpuCount: 1,
			}

			err := pgModel.Validate()
			assert.NoError(t, err)
		})
	}
}

func Test_Validate_WhenDatabaseIsNil_ValidatesSuccessfully(t *testing.T) {
	pgModel := &PostgresqlDatabase{
		Host:     "localhost",
		Port:     5437,
		Username: "postgres",
		Password: "somepassword",
		Database: nil,
		CpuCount: 1,
	}

	err := pgModel.Validate()
	assert.NoError(t, err)
}

func Test_Validate_WhenDatabaseIsEmpty_ValidatesSuccessfully(t *testing.T) {
	emptyDb := ""
	pgModel := &PostgresqlDatabase{
		Host:     "localhost",
		Port:     5437,
		Username: "postgres",
		Password: "somepassword",
		Database: &emptyDb,
		CpuCount: 1,
	}

	err := pgModel.Validate()
	assert.NoError(t, err)
}

func Test_Validate_WhenRequiredFieldsMissing_ReturnsError(t *testing.T) {
	testCases := []struct {
		name          string
		model         *PostgresqlDatabase
		expectedError string
	}{
		{
			name: "missing host",
			model: &PostgresqlDatabase{
				Host:     "",
				Port:     5432,
				Username: "user",
				Password: "pass",
				CpuCount: 1,
			},
			expectedError: "host is required",
		},
		{
			name: "missing port",
			model: &PostgresqlDatabase{
				Host:     "localhost",
				Port:     0,
				Username: "user",
				Password: "pass",
				CpuCount: 1,
			},
			expectedError: "port is required",
		},
		{
			name: "missing username",
			model: &PostgresqlDatabase{
				Host:     "localhost",
				Port:     5432,
				Username: "",
				Password: "pass",
				CpuCount: 1,
			},
			expectedError: "username is required",
		},
		{
			name: "missing password",
			model: &PostgresqlDatabase{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "",
				CpuCount: 1,
			},
			expectedError: "password is required",
		},
		{
			name: "invalid cpu count",
			model: &PostgresqlDatabase{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "pass",
				CpuCount: 0,
			},
			expectedError: "cpu count must be greater than 0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.model.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

type PostgresContainer struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	DB       *sqlx.DB
}

func connectToPostgresContainer(t *testing.T, port string) *PostgresContainer {
	dbName := "testdb"
	password := "testpassword"
	username := "testuser"
	host := config.GetEnv().TestLocalhost

	portInt, err := strconv.Atoi(port)
	assert.NoError(t, err)

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, portInt, username, password, dbName)

	db, err := sqlx.Connect("postgres", dsn)
	assert.NoError(t, err)

	var versionStr string
	err = db.Get(&versionStr, "SELECT version()")
	assert.NoError(t, err)

	return &PostgresContainer{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
		Database: dbName,
		DB:       db,
	}
}

func createPostgresModel(container *PostgresContainer) *PostgresqlDatabase {
	var versionStr string
	err := container.DB.Get(&versionStr, "SELECT version()")
	if err != nil {
		return nil
	}

	version := extractPostgresVersion(versionStr)

	return &PostgresqlDatabase{
		Version:  version,
		Host:     container.Host,
		Port:     container.Port,
		Username: container.Username,
		Password: container.Password,
		Database: &container.Database,
		IsHttps:  false,
		CpuCount: 1,
	}
}

func extractPostgresVersion(versionStr string) tools.PostgresqlVersion {
	if strings.Contains(versionStr, "PostgreSQL 12") {
		return tools.GetPostgresqlVersionEnum("12")
	} else if strings.Contains(versionStr, "PostgreSQL 13") {
		return tools.GetPostgresqlVersionEnum("13")
	} else if strings.Contains(versionStr, "PostgreSQL 14") {
		return tools.GetPostgresqlVersionEnum("14")
	} else if strings.Contains(versionStr, "PostgreSQL 15") {
		return tools.GetPostgresqlVersionEnum("15")
	} else if strings.Contains(versionStr, "PostgreSQL 16") {
		return tools.GetPostgresqlVersionEnum("16")
	} else if strings.Contains(versionStr, "PostgreSQL 17") {
		return tools.GetPostgresqlVersionEnum("17")
	}

	return tools.GetPostgresqlVersionEnum("16")
}
