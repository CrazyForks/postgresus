package postgresql

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"databasus-backend/internal/config"
	"databasus-backend/internal/util/tools"
)

func Test_TestConnection_PasswordContainingSpaces_TestedSuccessfully(t *testing.T) {
	env := config.GetEnv()
	container := connectToTestPostgresContainer(t, env.TestPostgres16Port)
	defer container.DB.Close()

	passwordWithSpaces := "test password with spaces"
	usernameWithSpaces := fmt.Sprintf("testuser_spaces_%s", uuid.New().String()[:8])

	_, err := container.DB.Exec(fmt.Sprintf(
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

type testPostgresContainer struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	DB       *sqlx.DB
}

func connectToTestPostgresContainer(t *testing.T, port string) *testPostgresContainer {
	dbName := "testdb"
	password := "testpassword"
	username := "testuser"
	host := "localhost"

	portInt, err := strconv.Atoi(port)
	assert.NoError(t, err)

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, portInt, username, password, dbName)

	db, err := sqlx.Connect("postgres", dsn)
	assert.NoError(t, err)

	return &testPostgresContainer{
		Host:     host,
		Port:     portInt,
		Username: username,
		Password: password,
		Database: dbName,
		DB:       db,
	}
}
