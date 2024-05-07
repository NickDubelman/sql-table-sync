package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLoadConfig(t *testing.T) {
	t.Run("load from string", func(t *testing.T) {
		cfg, err := loadConfig(`
            jobs:
              - name: users
                columns: [id, name, age]
                primaryKey: id
                source:
                  driver: sqlite3
                  dsn: ":memory:"
                  user: nick
                  password: greatsuccess
                  host: 0.0.0.0
                  port: 3420
                  db: appdb
                  table: users
                targets:
                  - host: 1.2.3.4
                    port: 3421
                    db: appdb-a
                    table: users2
                  - host: 5.6.7.8
                    port: 3422
                    db: appdb-b
                    table: users3
        `)
		require.NoError(t, err)
		require.Len(t, cfg.Jobs, 1)

		assert.Equal(t, "users", cfg.Jobs[0].Name)
		assert.Equal(t, []string{"id", "name", "age"}, cfg.Jobs[0].Columns)
		assert.Equal(t, "id", cfg.Jobs[0].PrimaryKey)

		source := cfg.Jobs[0].Source
		assert.Equal(t, "sqlite3", source.Driver)
		assert.Equal(t, ":memory:", source.DSN)
		assert.Equal(t, "nick", source.User)
		assert.Equal(t, "greatsuccess", source.Password)
		assert.Equal(t, "0.0.0.0", source.Host)
		assert.Equal(t, 3420, source.Port)
		assert.Equal(t, "appdb", source.DB)
		assert.Equal(t, "users", source.Table)

		targets := cfg.Jobs[0].Targets
		require.Len(t, targets, 2)
		assert.Empty(t, targets[0].DSN)
		assert.Equal(t, "1.2.3.4", targets[0].Host)
		assert.Equal(t, 3421, targets[0].Port)
		assert.Equal(t, "appdb-a", targets[0].DB)
		assert.Equal(t, "users2", targets[0].Table)
		assert.Empty(t, targets[1].DSN)
		assert.Equal(t, "5.6.7.8", targets[1].Host)
		assert.Equal(t, 3422, targets[1].Port)
		assert.Equal(t, "appdb-b", targets[1].DB)
		assert.Equal(t, "users3", targets[1].Table)
	})

	t.Run("default config values", func(t *testing.T) {
		cfg, err := loadConfig(`
            jobs:
              - name: users
                columns: [id, name, age]
        `)
		require.NoError(t, err)
		require.Len(t, cfg.Jobs, 1)

		// PrimaryKey is empty, so it should default to "id"
		assert.Equal(t, "id", cfg.Jobs[0].PrimaryKey)
	})

	t.Run("load from string (invalid)", func(t *testing.T) {
		_, err := loadConfig(`- abc`)
		assert.Error(t, err)
		var typeErr *yaml.TypeError
		assert.ErrorAs(t, err, &typeErr)
	})
}

func TestValidateConfig(t *testing.T) {
	t.Run("no jobs", func(t *testing.T) {
		cfg := Config{}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "no jobs found in config")
	})

	t.Run("job with no source table", func(t *testing.T) {
		cfg := Config{
			Jobs: []JobConfig{
				{Name: "users", Targets: []TableConfig{{Table: "users2"}}},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "job users has no source table")
	})

	t.Run("job with no targets", func(t *testing.T) {
		cfg := Config{
			Jobs: []JobConfig{
				{Name: "users", Source: TableConfig{Table: "users"}},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "job users has no targets")
	})

	t.Run("target with no table", func(t *testing.T) {
		cfg := Config{
			Jobs: []JobConfig{
				{
					Name:    "users",
					Source:  TableConfig{Table: "users"},
					Targets: []TableConfig{{Table: "abc"}, {Table: ""}},
				},
			},
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "job users, target[1] with no table")
	})
}
