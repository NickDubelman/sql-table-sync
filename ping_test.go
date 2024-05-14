package sync

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingAllJobs(t *testing.T) {
	sourceDSN := "file:test_ping_source.db?mode=memory&cache=shared"
	target1DSN := "file:test_ping_target1.db?mode=memory&cache=shared"
	target2DSN := "file:test_ping_target2.db?mode=memory&cache=shared"

	config := Config{
		Jobs: map[string]JobConfig{
			"users": {
				Columns: []string{"id", "name", "email"},
				Source: TableConfig{
					Driver: "sqlite3",
					DSN:    sourceDSN,
					Table:  "users",
				},
				Targets: []TableConfig{
					{
						Driver: "sqlite3",
						DSN:    target1DSN,
						Table:  "users",
					},
					{
						Driver: "sqlite3",
						DSN:    target2DSN,
						Table:  "users",
					},
				},
			},

			"pets": {
				Columns: []string{"id", "name", "user_id"},
				Source: TableConfig{
					Driver: "sqlite3",
					DSN:    sourceDSN,
					Table:  "pets",
				},
				Targets: []TableConfig{
					{
						Driver: "sqlite3",
						DSN:    target1DSN,
						Table:  "pets",
					},
				},
			},
		},
	}

	allResults, err := config.PingAllJobs(30 * time.Second)
	require.NoError(t, err)
	require.Len(t, allResults, 2)

	usersJobName := "users"
	require.Contains(t, allResults, usersJobName)
	usersResults := allResults[usersJobName]
	assert.Len(t, usersResults, 3)

	petsJobName := "pets"
	require.Contains(t, allResults, petsJobName)
	petsResults := allResults[petsJobName]
	assert.Len(t, petsResults, 2)

	// We haven't yet created the tables, so we expect them all to error
	for _, results := range allResults {
		for i, table := range results {
			if i == 0 {
				// First table should be the source
				assert.Equal(t, "source", table.Label)
			} else {
				// Each subsequent table should be a target
				assert.Contains(t, table.Label, "target")
			}

			assert.Error(t, table.Error)
			assert.ErrorContains(t, table.Error, "no such table")
		}
	}

	// After we create the tables, we should be able to ping them successfully
	sourceConn := sqlx.MustConnect("sqlite3", sourceDSN)
	defer sourceConn.Close()

	target1Conn := sqlx.MustConnect("sqlite3", target1DSN)
	defer target1Conn.Close()

	target2Conn := sqlx.MustConnect("sqlite3", target2DSN)
	defer target2Conn.Close()

	for _, conn := range []*sqlx.DB{sourceConn, target1Conn, target2Conn} {
		conn.MustExec(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);

			CREATE TABLE pets (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				user_id INTEGER NOT NULL
			);
		`)
	}

	allResults, err = config.PingAllJobs(30 * time.Second)
	require.NoError(t, err)
	require.Len(t, allResults, 2)

	usersResults = allResults[usersJobName]
	petsResults = allResults[petsJobName]
	assert.Len(t, usersResults, 3)
	assert.Len(t, petsResults, 2)

	for _, results := range allResults {
		for _, table := range results {
			assert.NoError(t, table.Error)
		}
	}
}

func TestPingAllJobs_mysql(t *testing.T) {
	dbName := os.Getenv("MYSQL_DB_NAME")
	dbPortStr := os.Getenv("MYSQL_DB_PORT")

	// MySQL DSNs
	dsn := fmt.Sprintf("root@tcp(localhost:%s)/%s", dbPortStr, dbName)

	config := Config{
		Jobs: map[string]JobConfig{
			"users": {
				PrimaryKeys: []string{"id"},
				Columns:     []string{"id", "name", "email"},
				Source: TableConfig{
					Driver: "mysql",
					Table:  "users_ping_all",
					DSN:    dsn,
				},
				Targets: []TableConfig{
					{
						Driver: "mysql",
						Table:  "users_ping_all_1",
						DSN:    dsn,
					},
					{
						Driver: "mysql",
						Table:  "users_ping_all_2",
						DSN:    dsn,
					},
				},
			},

			"pets": {
				Columns: []string{"id", "name", "user_id"},
				Source: TableConfig{
					Driver: "mysql",
					Table:  "pets_ping_all",
					DSN:    dsn,
				},
				Targets: []TableConfig{
					{
						Driver: "mysql",
						Table:  "pets_ping_all_1",
						DSN:    dsn,
					},
				},
			},
		},
	}

	allResults, err := config.PingAllJobs(30 * time.Second)
	require.NoError(t, err)
	require.Len(t, allResults, 2)

	usersJobName := "users"
	require.Contains(t, allResults, usersJobName)
	usersJob := config.Jobs[usersJobName]
	usersResults := allResults[usersJobName]
	assert.Len(t, usersResults, 3)

	petsJobName := "pets"
	require.Contains(t, allResults, petsJobName)
	petsJob := config.Jobs[petsJobName]
	petsResults := allResults[petsJobName]
	assert.Len(t, petsResults, 2)

	// We haven't yet created the tables, so we expect them all to error
	for _, results := range allResults {
		for i, table := range results {
			if i == 0 {
				// First table should be the source
				assert.Equal(t, "source", table.Label)
			} else {
				// Each subsequent table should be a target
				assert.Contains(t, table.Label, "target")
			}

			assert.Error(t, table.Error)
			assert.ErrorContains(t, table.Error, "doesn't exist")
		}
	}

	// After we create the tables, we should be able to ping them successfully
	conn := sqlx.MustConnect("mysql", dsn)
	defer conn.Close()

	createUsersTable := func(tableName string) {
		conn.MustExec(fmt.Sprintf(`
			CREATE TABLE %s (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			)
		`, tableName))
	}

	createPetsTable := func(tableName string) {
		conn.MustExec(fmt.Sprintf(`
			CREATE TABLE %s (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				user_id INTEGER NOT NULL
			)
		`, tableName))
	}

	createUsersTable(usersJob.Source.Table)
	for _, target := range usersJob.Targets {
		createUsersTable(target.Table)
	}

	createPetsTable(petsJob.Source.Table)
	for _, target := range petsJob.Targets {
		createPetsTable(target.Table)
	}

	allResults, err = config.PingAllJobs(30 * time.Second)
	require.NoError(t, err)
	require.Len(t, allResults, 2)

	usersResults = allResults[usersJobName]
	assert.Len(t, usersResults, 3)

	petsResults = allResults[petsJobName]
	assert.Len(t, petsResults, 2)

	for _, results := range allResults {
		for _, table := range results {
			assert.NoError(t, table.Error)
		}
	}
}

type sleepPingTarget struct {
	duration time.Duration
}

func (m sleepPingTarget) ping(columns []string) error {
	time.Sleep(m.duration)
	return nil
}

func TestPingWithTimeout(t *testing.T) {
	target := sleepPingTarget{duration: 500 * time.Millisecond}

	// Should error when the ping operation times out
	err := pingWithTimeout(100*time.Millisecond, target, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "ping operation timed out")

	// Should not error when the ping operation completes within the timeout
	err = pingWithTimeout(30*time.Second, target, nil)
	require.NoError(t, err)
}
