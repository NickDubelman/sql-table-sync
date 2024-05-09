package sync

import (
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	sourceDSN := "file:test_ping_source.db?mode=memory&cache=shared"
	target1DSN := "file:test_ping_target1.db?mode=memory&cache=shared"
	target2DSN := "file:test_ping_target2.db?mode=memory&cache=shared"

	config := Config{
		Jobs: []JobConfig{
			{
				Name:        "users",
				PrimaryKeys: []string{"id"},
				Columns:     []string{"id", "name", "email"},
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
		},
	}

	results, err := config.Ping(30 * time.Second)
	require.NoError(t, err)
	require.Len(t, results, 1)

	result := results[0]
	assert.Equal(t, "users", result.Job.Name)
	assert.Len(t, result.Tables, 3)

	// We haven't yet created the tables, so we expect them all to error
	for i, table := range result.Tables {
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
			)
		`)
	}

	results, err = config.Ping(30 * time.Second)
	require.NoError(t, err)
	require.Len(t, results, 1)

	result = results[0]
	assert.Equal(t, "users", result.Job.Name)
	assert.Len(t, result.Tables, 3)

	for _, table := range result.Tables {
		assert.NoError(t, table.Error)
	}
}

type sleepPingTarget struct {
	duration time.Duration
}

func (m sleepPingTarget) ping(columns []string) error {
	time.Sleep(m.duration)
	return nil
}

func TestPing_timeout(t *testing.T) {
	target := sleepPingTarget{duration: 500 * time.Millisecond}

	// Should error when the ping operation times out
	err := pingWithTimeout(100*time.Millisecond, target, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "ping operation timed out")

	// Should not error when the ping operation completes within the timeout
	err = pingWithTimeout(30*time.Second, target, nil)
	require.NoError(t, err)
}
