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
	type testCase struct {
		description string
		config      Config
		expectedErr string
	}

	testCases := []testCase{
		{
			description: "valid config",
			config: Config{
				Jobs: []JobConfig{
					{
						Name:        "users",
						Columns:     []string{"id", "name", "age"},
						PrimaryKeys: []string{"id"},
						Source: TableConfig{
							Table:  "users",
							Driver: "sqlite3",
						},
						Targets: []TableConfig{
							{
								Table:  "users2",
								Driver: "sqlite3",
							},
						},
					},
				},
			},
		},
		{
			description: "no jobs",
			config: Config{
				Jobs: nil,
			},
			expectedErr: "no jobs found in config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := tc.config.validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}

func TestValidateJobConfig(t *testing.T) {
	validJob := func() JobConfig {
		return JobConfig{
			Name:        "users",
			Columns:     []string{"id", "name", "age"},
			PrimaryKeys: []string{"id"},
			Source: TableConfig{
				Table:  "users",
				Driver: "sqlite3",
			},
			Targets: []TableConfig{
				{
					Table:  "users2",
					Driver: "sqlite3",
				},
			},
		}
	}

	type testCase struct {
		description string
		job         func() JobConfig
		expectedErr string
	}

	testCases := []testCase{
		{
			description: "valid job",
			job:         validJob,
		},
		{
			description: "missing name",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Name = ""
				return cfg
			},
			expectedErr: "job has no name",
		},
		{
			description: "missing columns",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Columns = nil
				return cfg
			},
			expectedErr: "job 'users' does not specify any columns",
		},
		{
			description: "missing primary keys",
			job: func() JobConfig {
				cfg := validJob()
				cfg.PrimaryKeys = nil
				return cfg
			},
			expectedErr: "job 'users' has no primary keys",
		},
		{
			description: "too many primary keys",
			job: func() JobConfig {
				cfg := validJob()
				cfg.PrimaryKeys = []string{"id", "name", "age", "favoriteColor"}
				return cfg
			},
			expectedErr: "job 'users' has too many primary keys",
		},
		{
			description: "primary key not in columns",
			job: func() JobConfig {
				cfg := validJob()
				cfg.PrimaryKeys = []string{"favoriteColor"}
				return cfg
			},
			expectedErr: "job 'users' has primary key 'favoriteColor' not in columns",
		},
		{
			description: "missing source table",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Source.Table = ""
				return cfg
			},
			expectedErr: "job 'users' source: table name is empty",
		},
		{
			description: "missing source driver",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Source.Driver = ""
				return cfg
			},
			expectedErr: "job 'users' source: table does not specify a driver",
		},
		{
			description: "missing targets",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets = nil
				return cfg
			},
			expectedErr: "job 'users' has no targets",
		},
		{
			description: "missing target table",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets[0].Table = ""
				return cfg
			},
			expectedErr: "job 'users' target[0]: table name is empty",
		},
		{
			description: "missing target driver",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets[0].Driver = ""
				return cfg
			},
			expectedErr: "job 'users' target[0]: table does not specify a driver",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := tc.job().validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}

func TestValidateTableConfig(t *testing.T) {
	validTable := func() TableConfig {
		return TableConfig{
			Table:  "users",
			Driver: "sqlite3",
		}
	}

	type testCase struct {
		description string
		table       func() TableConfig
		expectedErr string
	}

	testCases := []testCase{
		{
			description: "valid table",
			table:       validTable,
		},
		{
			description: "missing table name",
			table: func() TableConfig {
				cfg := validTable()
				cfg.Table = ""
				return cfg
			},
			expectedErr: "table name is empty",
		},
		{
			description: "missing table driver",
			table: func() TableConfig {
				cfg := validTable()
				cfg.Driver = ""
				return cfg
			},
			expectedErr: "table does not specify a driver",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := tc.table().validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}
