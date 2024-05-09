package sync

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecJob(t *testing.T) {
	createTable := `
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			age INT NOT NULL
		)
	`

	sourceConfig := TableConfig{
		Driver: "sqlite3",
		Table:  "users",
		DSN:    "file:exec_job_source.db?mode=memory&cache=shared",
	}

	source := table{config: sourceConfig}
	source.connect()
	source.MustExec(createTable)

	expectedData := [][]any{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	insert := squirrel.Insert(sourceConfig.Table).Columns("id", "name", "age")

	for _, row := range expectedData {
		insert = insert.Values(row...)
	}

	sql, args, err := insert.ToSql()
	require.NoError(t, err)

	// Insert some data into the source
	source.MustExec(sql, args...)

	target1Config := TableConfig{
		Driver: "sqlite3",
		Table:  "users",
		DSN:    "file:exec_job_target1.db?mode=memory&cache=shared",
	}

	target1 := table{config: target1Config}
	target1.connect()
	target1.MustExec(createTable)

	// target1 has some data that needs to be updated/deleted
	target1.MustExec("INSERT INTO users (id, name, age) VALUES (1, 'Nick', 31)")
	target1.MustExec("INSERT INTO users (id, name, age) VALUES (420, 'Azamat', 69)")

	target2Config := TableConfig{
		Driver: "sqlite3",
		Table:  "users",
		DSN:    "file:exec_job_target2.db?mode=memory&cache=shared",
	}

	target2 := table{config: target2Config}
	target2.connect()
	target2.MustExec(createTable)

	// target2 has no data

	target3Config := TableConfig{
		Label:  "already in sync",
		Driver: "sqlite3",
		Table:  "users",
		DSN:    "file:exec_job_target3.db?mode=memory&cache=shared",
	}

	target3 := table{config: target3Config}
	target3.connect()
	target3.MustExec(createTable)

	// table3 is already in sync
	target3.MustExec(sql, args...)

	config := Config{
		Jobs: []JobConfig{
			{
				Name:        "users",
				PrimaryKeys: []string{"id"},
				Columns:     []string{"id", "name", "age"},
				Source:      sourceConfig,
				Targets:     []TableConfig{target1Config, target2Config, target3Config},
			},
		},
	}

	results, err := config.ExecJob("users")
	require.NoError(t, err)
	require.Len(t, results.Results, 3)

	for _, result := range results.Results {
		assert.NoError(t, result.Error)

		if result.Target.Label == "already in sync" {
			assert.False(t, result.Synced)
		} else {
			assert.True(t, result.Synced)
		}
	}

	// Check that the data was copied to each target
	for _, target := range []table{target1, target2, target3} {
		rows, err := target.Queryx("SELECT * FROM users")
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

func TestExecJob_multiple_primary_key(t *testing.T) {
	createTable := `
		CREATE TABLE IF NOT EXISTS users (
			name TEXT NOT NULL,
			age INT NOT NULL,
			favoriteColor TEXT NOT NULL,
			PRIMARY KEY (age, name)
		)
	`

	sourceConfig := TableConfig{
		Driver: "sqlite3",
		Table:  "users",
		DSN:    "file:exec_job_multi_pk_source.db?mode=memory&cache=shared",
	}

	source := table{config: sourceConfig}
	source.connect()
	source.MustExec(createTable)

	expectedData := [][]any{
		{"Bob", 25, "blue"},
		{"Alice", 30, "red"},
		{"Charlie", 35, "green"},
	}

	insert := squirrel.Insert(sourceConfig.Table).Columns("name", "age", "favoriteColor")

	for _, row := range expectedData {
		insert = insert.Values(row...)
	}

	sql, args, err := insert.ToSql()
	require.NoError(t, err)

	// Insert some data into the source
	source.MustExec(sql, args...)

	target1Config := TableConfig{
		Driver: "sqlite3",
		Table:  "users",
		DSN:    "file:exec_job_multi_pk_target1.db?mode=memory&cache=shared",
	}

	target1 := table{config: target1Config}
	target1.connect()
	target1.MustExec(createTable)

	// target1 has no data

	primaryKeys := []string{"age", "name"}

	config := Config{
		Jobs: []JobConfig{
			{
				Name:        "users",
				PrimaryKeys: primaryKeys,
				Columns:     []string{"name", "age", "favoriteColor"},
				Source:      sourceConfig,
				Targets:     []TableConfig{target1Config},
			},
		},
	}

	results, err := config.ExecJob("users")
	require.NoError(t, err)
	require.Len(t, results.Results, 1)

	for _, result := range results.Results {
		assert.NoError(t, result.Error)
		assert.True(t, result.Synced)
	}

	// Check that the data was copied to the target
	order := strings.Join(primaryKeys, ", ")
	rows, err := target1.Queryx("SELECT * FROM users ORDER BY " + order)
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

func TestExecJob_mysql(t *testing.T) {
	dbName := os.Getenv("MYSQL_DB_NAME")
	dbPortStr := os.Getenv("MYSQL_DB_PORT")
	dbPort, _ := strconv.Atoi(dbPortStr)

	createTable := func(name string) string {
		return fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY NOT NULL,
				name TEXT NOT NULL,
				age INT NOT NULL
			)
		`, name)
	}

	sourceConfig := TableConfig{
		Driver: "mysql",
		Table:  "users",
		User:   "root",
		DB:     dbName,
		Port:   dbPort,
	}

	source := table{config: sourceConfig}
	err := source.connect()
	require.NoError(t, err)
	source.MustExec(createTable(sourceConfig.Table))

	expectedData := [][]any{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	insert := squirrel.Insert(sourceConfig.Table).Columns("id", "name", "age")

	for _, row := range expectedData {
		insert = insert.Values(row...)
	}

	sql, args, err := insert.ToSql()
	require.NoError(t, err)

	// Insert some data into the source
	source.MustExec(sql, args...)

	target1Config := TableConfig{
		Driver: "mysql",
		Table:  "users2",
		User:   "root",
		DB:     dbName,
		Port:   dbPort,
	}

	target1 := table{config: target1Config}
	err = target1.connect()
	require.NoError(t, err)
	target1.MustExec(createTable(target1Config.Table))

	// target1 has some data that needs to be updated/deleted
	target1.MustExec(
		fmt.Sprintf(
			"INSERT INTO %s (id, name, age) VALUES (1, 'Nick', 31)",
			target1Config.Table,
		),
	)
	target1.MustExec(
		fmt.Sprintf(
			"INSERT INTO %s (id, name, age) VALUES (420, 'Azamat', 69)",
			target1Config.Table,
		),
	)

	target2Config := TableConfig{
		Driver: "mysql",
		Table:  "users3",
		User:   "root",
		DB:     dbName,
		Port:   dbPort,
	}

	target2 := table{config: target2Config}
	err = target2.connect()
	require.NoError(t, err)
	target2.MustExec(createTable(target2Config.Table))

	// target2 has no data

	target3Config := TableConfig{
		Label:  "already in sync",
		Driver: "mysql",
		Table:  "users4",
		User:   "root",
		DB:     dbName,
		Port:   dbPort,
	}

	target3 := table{config: target3Config}
	err = target3.connect()
	require.NoError(t, err)
	target3.MustExec(createTable(target3Config.Table))

	// table3 is already in sync
	insert = squirrel.Insert(target3Config.Table).Columns("id", "name", "age")

	for _, row := range expectedData {
		insert = insert.Values(row...)
	}

	sql, args, err = insert.ToSql()
	require.NoError(t, err)
	target3.MustExec(sql, args...)

	config := Config{
		Jobs: []JobConfig{
			{
				Name:        "users",
				PrimaryKeys: []string{"id"},
				Columns:     []string{"id", "name", "age"},
				Source:      sourceConfig,
				Targets:     []TableConfig{target1Config, target2Config, target3Config},
			},
		},
	}

	results, err := config.ExecJob("users")
	require.NoError(t, err)
	require.Len(t, results.Results, 3)

	for _, result := range results.Results {
		assert.NoError(t, result.Error)

		if result.Target.Label == "already in sync" {
			assert.False(t, result.Synced)
		} else {
			assert.True(t, result.Synced)
		}
	}

	// Check that the data was copied to each target
	for _, target := range []table{target1, target2, target3} {
		rows, err := target.Queryx("SELECT * FROM users")
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
