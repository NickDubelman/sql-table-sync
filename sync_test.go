package sync

import (
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
)

func TestSync(t *testing.T) {
	source, err := Connect(TableConfig{Driver: "sqlite3", DSN: ":memory:", Table: "users"})
	require.NoError(t, err)

	target1, err := Connect(TableConfig{Driver: "sqlite3", DSN: ":memory:", Table: "users"})
	require.NoError(t, err)

	target2, err := Connect(TableConfig{Driver: "sqlite3", DSN: ":memory:", Table: "users"})
	require.NoError(t, err)

	targets := []Table{target1, target2}

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

	// Insert some data to update into one of the targets
	target1.MustExec("INSERT INTO users (id, name, age) VALUES (1, 'Nick', 31)")

	// Insert some data to delete into one of the targets
	target1.MustExec("INSERT INTO users (id, name, age) VALUES (420, 'Azamat', 69)")

	// table2 has no data

	err = sync(
		"id",
		[]string{"id", "name", "age"},
		source,
		targets,
	)
	require.NoError(t, err)

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

		// Make sure the data is correct
		for i := range data {
			for j := range data[i] {
				require.EqualValues(t, expectedData[i][j], data[i][j])
			}
		}
	}
}
