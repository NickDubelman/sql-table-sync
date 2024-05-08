package sync

import (
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncTargets(t *testing.T) {
	source := Table{
		Config: TableConfig{
			Driver: "sqlite3",
			DSN:    ":memory:",
			Table:  "users",
		},
	}

	target1 := Table{
		Config: TableConfig{
			Driver: "sqlite3",
			DSN:    ":memory:",
			Table:  "users",
		},
	}

	target2 := Table{
		Config: TableConfig{
			Driver: "sqlite3",
			DSN:    ":memory:",
			Table:  "users",
		},
	}

	target3 := Table{
		Config: TableConfig{
			Label:  "already in sync",
			Driver: "sqlite3",
			DSN:    ":memory:",
			Table:  "users",
		},
	}

	err := source.connect()
	require.NoError(t, err)

	err = target1.connect()
	require.NoError(t, err)

	err = target2.connect()
	require.NoError(t, err)

	err = target3.connect()
	require.NoError(t, err)

	targets := []Table{target1, target2, target3}

	// Create a users table in the source and each target
	for _, table := range append(targets, source) {
		table.MustExec(`
			CREATE TABLE IF NOT EXISTS users (
				id INTEGER PRIMARY KEY NOT NULL,
				name TEXT NOT NULL,
				age INT NOT NULL
			)
		`)
	}

	expectedData := [][]any{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	insert := squirrel.
		Insert(source.Config.Table).
		Columns("id", "name", "age")

	for _, row := range expectedData {
		insert = insert.Values(row...)
	}

	sql, args, err := insert.ToSql()
	require.NoError(t, err)

	// Insert some data into the source
	source.MustExec(sql, args...)

	// table3 is already in sync
	target3.MustExec(sql, args...)

	// Insert some data to update into one of the targets
	target1.MustExec("INSERT INTO users (id, name, age) VALUES (1, 'Nick', 31)")

	// Insert some data to delete into one of the targets
	target1.MustExec("INSERT INTO users (id, name, age) VALUES (420, 'Azamat', 69)")

	// target2 has no data

	_, results, err := syncTargets(
		source,
		targets,
		[]string{"id"},
		[]string{"id", "name", "age"},
	)
	require.NoError(t, err)
	assert.Len(t, results, 3)

	for _, result := range results {
		assert.NoError(t, result.Error)

		if result.Target.Config.Label == "already in sync" {
			assert.False(t, result.Synced)
		} else {
			assert.True(t, result.Synced)
		}
	}

	// Check that the data was copied to each target
	for _, table := range targets {
		rows, err := table.Queryx("SELECT * FROM users")
		require.NoError(t, err)

		defer rows.Close()

		var data [][]any
		for rows.Next() {
			cols, err := rows.SliceScan()
			require.NoError(t, err)
			data = append(data, cols)
		}

		require.Equal(t, len(expectedData), len(data))

		// Make sure the data is correct
		for i := range expectedData {
			require.Len(t, data[i], len(expectedData[i]))
			for j := range expectedData[i] {
				require.EqualValues(t, expectedData[i][j], data[i][j])
			}
		}
	}
}

func TestSyncTargets_multiple_primary_key(t *testing.T) {
	source := Table{
		Config: TableConfig{
			Driver: "sqlite3",
			DSN:    ":memory:",
			Table:  "users",
		},
	}

	target1 := Table{
		Config: TableConfig{
			Driver: "sqlite3",
			DSN:    ":memory:",
			Table:  "users",
		},
	}

	err := source.connect()
	require.NoError(t, err)

	err = target1.connect()
	require.NoError(t, err)

	targets := []Table{target1}

	// Create a users table in the source and each target
	for _, table := range append(targets, source) {
		table.MustExec(`
			CREATE TABLE IF NOT EXISTS users (
				name TEXT NOT NULL,
				age INT NOT NULL,
				favoriteColor TEXT NOT NULL,
				PRIMARY KEY (age, name)
			)
		`)
	}

	expectedData := [][]any{
		{"Bob", 25, "blue"},
		{"Alice", 30, "red"},
		{"Charlie", 35, "green"},
	}

	insert := squirrel.
		Insert(source.Config.Table).
		Columns("name", "age", "favoriteColor")

	for _, row := range expectedData {
		insert = insert.Values(row...)
	}

	sql, args, err := insert.ToSql()
	require.NoError(t, err)

	// Insert some data into the source
	source.MustExec(sql, args...)

	// target2 has no data

	primaryKeys := []string{"age", "name"}

	_, results, err := syncTargets(
		source,
		targets,
		primaryKeys,
		[]string{"name", "age", "favoriteColor"},
	)
	require.NoError(t, err)
	assert.Len(t, results, 1)

	for _, result := range results {
		assert.NoError(t, result.Error)
		assert.True(t, result.Synced)
	}

	// Check that the data was copied to each target
	for _, table := range targets {
		order := strings.Join(primaryKeys, ", ")
		rows, err := table.Queryx("SELECT * FROM users ORDER BY " + order)
		require.NoError(t, err)

		defer rows.Close()

		var data [][]any
		for rows.Next() {
			cols, err := rows.SliceScan()
			require.NoError(t, err)
			data = append(data, cols)
		}

		require.Equal(t, len(expectedData), len(data))

		// Make sure the data is correct
		for i := range expectedData {
			require.Len(t, data[i], len(expectedData[i]))
			for j := range expectedData[i] {
				require.EqualValues(t, expectedData[i][j], data[i][j])
			}
		}
	}
}
