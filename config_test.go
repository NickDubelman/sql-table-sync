package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLoadConfig(t *testing.T) {
	t.Run("load invalid config", func(t *testing.T) {
		_, err := loadConfig(`- abc`)
		assert.Error(t, err)
		var typeErr *yaml.TypeError
		assert.ErrorAs(t, err, &typeErr)
	})

	t.Run("load valid config", func(t *testing.T) {
		cfg, err := loadConfig(`
            jobs:
              users:
                columns: [id, name, age]
                primaryKey: id
                source:
                  driver: sqlite3
                  dsn: "my_fake_dsn"
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
		jobName := "users"
		require.Contains(t, cfg.Jobs, jobName)
		job := cfg.Jobs[jobName]

		assert.Equal(t, []string{"id", "name", "age"}, job.Columns)
		assert.Equal(t, "id", job.PrimaryKey)

		source := job.Source
		assert.Equal(t, "sqlite3", source.Driver)
		assert.Equal(t, "my_fake_dsn", source.DSN)
		assert.Equal(t, "nick", source.User)
		assert.Equal(t, "greatsuccess", source.Password)
		assert.Equal(t, "0.0.0.0", source.Host)
		assert.Equal(t, 3420, source.Port)
		assert.Equal(t, "appdb", source.DB)
		assert.Equal(t, "users", source.Table)

		targets := job.Targets
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
            defaults:
              driver: sqlite3
              hosts:
                host1:
                    driver: mysql
                    user: user1
                    password: pass1
                    port: 3369
                    db: appdb
                host2:
                    driver: postgres
                    dsn: host2_dsn
                host3:
                    label: host3_label
                    user: user3
                    password: pass3

            jobs:
              users:
                columns: [id, name, age]
                source:
                  host: host1
                  table: users
                targets:
                  - host: host2
                    table: users
                  - host: host3
                    dsn: host3_dsn
                    table: users

              pets:
                columns: [id, name, userID]
                source:
                  host: host1
                  table: pets
                targets:
                  - host: host2
                    dsn: host2_dsn_override
                    table: pets
                  - label: host3_label_override
                    host: host3
                    user: user3_override
                    password: pass3_override
                    table: pets
                  - host: host4
                    table: pets
                  - port: 1234
                    table: pets
        `)
		require.NoError(t, err)
		require.Len(t, cfg.Jobs, 2)

		usersJobName := "users"
		require.Contains(t, cfg.Jobs, usersJobName)
		usersJob := cfg.Jobs[usersJobName]

		petsJobName := "pets"
		require.Contains(t, cfg.Jobs, petsJobName)
		petsJob := cfg.Jobs[petsJobName]

		assert.Equal(t, "host1:3369", usersJob.Source.Label)
		assert.Equal(t, "mysql", usersJob.Source.Driver)
		assert.Equal(t, "user1", usersJob.Source.User)
		assert.Equal(t, "pass1", usersJob.Source.Password)
		assert.Equal(t, 3369, usersJob.Source.Port)
		assert.Equal(t, "appdb", usersJob.Source.DB)

		assert.Equal(t, "host2_dsn", usersJob.Targets[0].Label)
		assert.Equal(t, "postgres", usersJob.Targets[0].Driver)
		assert.Equal(t, "host2_dsn", usersJob.Targets[0].DSN)

		assert.Equal(t, "host3_label", usersJob.Targets[1].Label)
		assert.Equal(t, "sqlite3", usersJob.Targets[1].Driver)
		assert.Equal(t, "host3_dsn", usersJob.Targets[1].DSN)

		assert.Equal(t, "host1:3369", petsJob.Source.Label)
		assert.Equal(t, "mysql", petsJob.Source.Driver)
		assert.Equal(t, "user1", petsJob.Source.User)
		assert.Equal(t, "pass1", petsJob.Source.Password)
		assert.Equal(t, 3369, petsJob.Source.Port)
		assert.Equal(t, "appdb", petsJob.Source.DB)

		assert.Equal(t, "host2_dsn_override", petsJob.Targets[0].Label)
		assert.Equal(t, "postgres", petsJob.Targets[0].Driver)
		assert.Equal(t, "host2_dsn_override", petsJob.Targets[0].DSN)

		assert.Equal(t, "host3_label_override", petsJob.Targets[1].Label)
		assert.Equal(t, "sqlite3", petsJob.Targets[1].Driver)
		assert.Equal(t, "user3_override", petsJob.Targets[1].User)
		assert.Equal(t, "pass3_override", petsJob.Targets[1].Password)

		assert.Equal(t, "host4", petsJob.Targets[2].Label)
		assert.Equal(t, ":1234", petsJob.Targets[3].Label)
	})
}

func TestValidateConfig(t *testing.T) {
	validConfig := func() Config {
		return Config{
			Jobs: map[string]JobConfig{
				"users": {
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
		}
	}

	type testCase struct {
		description string
		config      func() Config
		expectedErr string
	}

	testCases := []testCase{
		{
			description: "valid config",
			config:      validConfig,
		},
		{
			description: "no jobs",
			config: func() Config {
				cfg := validConfig()
				cfg.Jobs = nil
				return cfg
			},
			expectedErr: "no jobs found in config",
		},
		{
			description: "empty job name",
			config: func() Config {
				cfg := validConfig()
				cfg.Jobs[""] = JobConfig{}
				return cfg
			},
			expectedErr: "all jobs must have a name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := tc.config().validate()
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
			description: "missing columns",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Columns = nil
				return cfg
			},
			expectedErr: "does not specify any columns",
		},
		{
			description: "missing primary keys",
			job: func() JobConfig {
				cfg := validJob()
				cfg.PrimaryKeys = nil
				return cfg
			},
			expectedErr: "has no primary keys",
		},
		{
			description: "too many primary keys",
			job: func() JobConfig {
				cfg := validJob()
				cfg.PrimaryKeys = []string{"id", "name", "age", "favoriteColor"}
				return cfg
			},
			expectedErr: "has too many primary keys",
		},
		{
			description: "primary key not in columns",
			job: func() JobConfig {
				cfg := validJob()
				cfg.PrimaryKeys = []string{"favoriteColor"}
				return cfg
			},
			expectedErr: "has primary key 'favoriteColor' not in columns",
		},
		{
			description: "missing source table",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Source.Table = ""
				return cfg
			},
			expectedErr: "source: table name is empty",
		},
		{
			description: "missing source driver",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Source.Driver = ""
				return cfg
			},
			expectedErr: "source: table does not specify a driver",
		},
		{
			description: "missing targets",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets = nil
				return cfg
			},
			expectedErr: "has no targets",
		},
		{
			description: "missing target table",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets[0].Table = ""
				return cfg
			},
			expectedErr: "target[0]: table name is empty",
		},
		{
			description: "missing target driver",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets[0].Driver = ""
				return cfg
			},
			expectedErr: "target[0]: table does not specify a driver",
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
		{
			description: "DSN and other connection parameters",
			table: func() TableConfig {
				cfg := validTable()
				cfg.DSN = "my_fake_dsn"
				cfg.User = "nick"
				return cfg
			},
			expectedErr: "table cannot specify DSN and other connection parameters",
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
