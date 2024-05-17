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
              
              source:
                  host: host1

              hosts:
                host1:
                    driver: mysql
                    user: user1
                    password: pass1
                    port: 3369
                    db: appdb
                host2:
                    driver: postgres
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
                  - host: host3
                    dsn: host3_dsn
                    table: users

              pets:
                columns: [id, name, userID]
                source:
                  table: pets
                targets:
                  - host: host2
                    table: pets_override
                  - label: host3_label_override
                    host: host3
                    port: 69420
                    user: user3_override
                    password: pass3_override
                    table: pets
                  - host: host4
                    table: pets
                  - port: 1234
                    table: pets

              posts:
                columns: [id, title, body]
                source:
                  dsn: posts_dsn
                  table: posts
                targets:
                  - dsn: posts_dsn2
        `)
		require.NoError(t, err)
		require.Len(t, cfg.Jobs, 3)

		usersJobName := "users"
		require.Contains(t, cfg.Jobs, usersJobName)
		usersJob := cfg.Jobs[usersJobName]

		petsJobName := "pets"
		require.Contains(t, cfg.Jobs, petsJobName)
		petsJob := cfg.Jobs[petsJobName]

		postsJobName := "posts"
		require.Contains(t, cfg.Jobs, postsJobName)
		postsJob := cfg.Jobs[postsJobName]

		assert.Equal(t, "host1:3369", usersJob.Source.Label)
		assert.Equal(t, "mysql", usersJob.Source.Driver)
		assert.Equal(t, "user1", usersJob.Source.User)
		assert.Equal(t, "pass1", usersJob.Source.Password)
		assert.Equal(t, 3369, usersJob.Source.Port)
		assert.Equal(t, "appdb", usersJob.Source.DB)

		assert.Equal(t, "host2", usersJob.Targets[0].Label)
		assert.Equal(t, "postgres", usersJob.Targets[0].Driver)
		// Make sure target's table defaults to source's table when hosts are different
		assert.Equal(t, "users", usersJob.Targets[0].Table)

		assert.Equal(t, "host3_label", usersJob.Targets[1].Label)
		assert.Equal(t, "sqlite3", usersJob.Targets[1].Driver)
		assert.Equal(t, "host3_dsn", usersJob.Targets[1].DSN)

		// Make sure pet job has the default host (since no Host or DSN is provided)
		assert.Equal(t, "host1", petsJob.Source.Host)

		assert.Equal(t, "host1:3369", petsJob.Source.Label)
		assert.Equal(t, "mysql", petsJob.Source.Driver)
		assert.Equal(t, "user1", petsJob.Source.User)
		assert.Equal(t, "pass1", petsJob.Source.Password)
		assert.Equal(t, 3369, petsJob.Source.Port)
		assert.Equal(t, "appdb", petsJob.Source.DB)

		assert.Equal(t, "host2", petsJob.Targets[0].Label)
		assert.Equal(t, "postgres", petsJob.Targets[0].Driver)
		assert.Equal(t, "pets_override", petsJob.Targets[0].Table)

		assert.Equal(t, "host3", petsJob.Targets[1].Host)
		assert.Equal(t, 69420, petsJob.Targets[1].Port)
		assert.Equal(t, "host3_label_override", petsJob.Targets[1].Label)
		assert.Equal(t, "sqlite3", petsJob.Targets[1].Driver)
		assert.Equal(t, "user3_override", petsJob.Targets[1].User)
		assert.Equal(t, "pass3_override", petsJob.Targets[1].Password)

		assert.Equal(t, "host4", petsJob.Targets[2].Label)
		assert.Equal(t, ":1234", petsJob.Targets[3].Label)

		// Make sure target defaults to same table name as source when DSNs are different
		assert.Equal(t, "posts_dsn", postsJob.Source.DSN)
		assert.Equal(t, "posts", postsJob.Source.Table)
		assert.Equal(t, "posts_dsn2", postsJob.Targets[0].DSN)
		assert.Equal(t, "posts", postsJob.Targets[0].Table)

		// Make sure posts does not have a default host since it has a DSN
		assert.Empty(t, postsJob.Source.Host)
	})

	t.Run("default source and targets", func(t *testing.T) {
		cfg, err := loadConfig(`
            defaults:              
              source:
                host: host1
                label: source_label
                driver: host1_driver
                user: host1_user
                password: host1_pass
                port: 1
                db: host1_db

              targets:
                - dsn: host2_dsn
                  label: host2_label
                  driver: host2_driver
                - host: host3
                  label: host3_label
                  driver: host3_driver
                  user: host3_user
                  password: host3_pass
                  port: 3
                  db: host3_db

            jobs:
              users:
                columns: [id, name, age]
                source:
                  table: users

              pets:
                columns: [id, name, userID]
                source:
                  host: host1_override
                  label: source_label_override
                  driver: host1_driver_override
                  user: host1_user_override
                  password: host1_pass_override
                  port: 42069
                  db: host1_db_override
                  table: pets
                targets:
                  - dsn: target2_override
                  - dsn: target3_override
                  - dsn: target4_override
                  - dsn: target5_override
                  
        `)
		require.NoError(t, err)
		require.Len(t, cfg.Jobs, 2)

		usersJobName := "users"
		require.Contains(t, cfg.Jobs, usersJobName)
		usersJob := cfg.Jobs[usersJobName]

		petsJobName := "pets"
		require.Contains(t, cfg.Jobs, petsJobName)
		petsJob := cfg.Jobs[petsJobName]

		// Users job should get default source host
		assert.Equal(t, "host1", usersJob.Source.Host)
		assert.Equal(t, "source_label", usersJob.Source.Label)
		assert.Equal(t, "host1_driver", usersJob.Source.Driver)
		assert.Equal(t, "host1_user", usersJob.Source.User)
		assert.Equal(t, "host1_pass", usersJob.Source.Password)
		assert.Equal(t, 1, usersJob.Source.Port)
		assert.Equal(t, "host1_db", usersJob.Source.DB)

		// Users job should get default targets
		require.Len(t, usersJob.Targets, 2)

		// Host2 target should get its values from defaults.targets[0]
		assert.Empty(t, usersJob.Targets[0].Host)
		assert.Equal(t, "host2_label", usersJob.Targets[0].Label)
		assert.Equal(t, "host2_driver", usersJob.Targets[0].Driver)
		assert.Equal(t, "host2_dsn", usersJob.Targets[0].DSN)

		// Host3 target should get its values from defaults.targets[1]
		assert.Equal(t, "host3", usersJob.Targets[1].Host)
		assert.Equal(t, "host3_label", usersJob.Targets[1].Label)
		assert.Equal(t, "host3_driver", usersJob.Targets[1].Driver)
		assert.Equal(t, "host3_user", usersJob.Targets[1].User)
		assert.Equal(t, "host3_pass", usersJob.Targets[1].Password)
		assert.Equal(t, 3, usersJob.Targets[1].Port)
		assert.Equal(t, "host3_db", usersJob.Targets[1].DB)

		// Pets job should get overridden source host
		assert.Equal(t, "host1_override", petsJob.Source.Host)
		assert.Equal(t, "source_label_override", petsJob.Source.Label)
		assert.Equal(t, "host1_driver_override", petsJob.Source.Driver)
		assert.Equal(t, "host1_user_override", petsJob.Source.User)
		assert.Equal(t, "host1_pass_override", petsJob.Source.Password)
		assert.Equal(t, 42069, petsJob.Source.Port)
		assert.Equal(t, "host1_db_override", petsJob.Source.DB)
		assert.Equal(t, "pets", petsJob.Source.Table)

		// Pets job should get overridden targets
		require.Len(t, petsJob.Targets, 4)
		assert.Equal(t, "target2_override", petsJob.Targets[0].DSN)
		assert.Equal(t, "target3_override", petsJob.Targets[1].DSN)
		assert.Equal(t, "target4_override", petsJob.Targets[2].DSN)
		assert.Equal(t, "target5_override", petsJob.Targets[3].DSN)
	})

	t.Run("default source and targets (with default hosts)", func(t *testing.T) {
		cfg, err := loadConfig(`
            defaults:
              driver: sqlite3
              
              source:
                # These should override the stuff specified in hosts.host1
                host: host1
                label: source_label
                driver: host1_driver
                user: host1_user
                password: host1_pass
                port: 42069
                db: host1_db

              targets:
                - host: host2
                - host: host3
                  label: host3_label
                  driver: host3_driver
                  user: host3_user
                  password: host3_pass
                  port: 3
                  db: host3_db

              hosts:
                host1:
                  driver: mysql
                  user: user1
                  password: pass1
                  port: 3369
                  db: appdb
                host2:
                  label: host2_label
                  driver: postgres
                  user: user2
                  password: pass2
                  port: 2
                  db: host2_db
                host3:
                  label: host3_label_not_used
                  port: 69420
                  user: user3_not_used
                  password: pass3_not_used
                  db: host3_db_not_used

            jobs:
              users:
                columns: [id, name, age]
                source:
                  table: users

              pets:
                columns: [id, name, userID]
                source:
                  dsn: host1_dsn_override
                  table: pets
                  
              posts:
                columns: [id, title, body]
                source:
                  host: host1_override
                  table: posts
                targets:
                  - dsn: posts_dsn2
                  - dsn: posts_dsn3
        `)
		require.NoError(t, err)
		require.Len(t, cfg.Jobs, 3)

		usersJobName := "users"
		require.Contains(t, cfg.Jobs, usersJobName)
		usersJob := cfg.Jobs[usersJobName]

		petsJobName := "pets"
		require.Contains(t, cfg.Jobs, petsJobName)
		petsJob := cfg.Jobs[petsJobName]

		postsJobName := "posts"
		require.Contains(t, cfg.Jobs, postsJobName)
		postsJob := cfg.Jobs[postsJobName]

		// Users job should get default source host
		assert.Equal(t, "host1", usersJob.Source.Host)

		// Users job source should get its values from defaults.source, not defaults.hosts.host1
		assert.Equal(t, "source_label", usersJob.Source.Label)
		assert.Equal(t, "host1_driver", usersJob.Source.Driver)
		assert.Equal(t, "host1_user", usersJob.Source.User)
		assert.Equal(t, "host1_pass", usersJob.Source.Password)
		assert.Equal(t, 42069, usersJob.Source.Port)
		assert.Equal(t, "host1_db", usersJob.Source.DB)

		// Users job should get default targets
		require.Len(t, usersJob.Targets, 2)

		// Host2 target should get its values from defaults.hosts.host2
		assert.Equal(t, "host2", usersJob.Targets[0].Host)
		assert.Equal(t, "host2_label", usersJob.Targets[0].Label)
		assert.Equal(t, "postgres", usersJob.Targets[0].Driver)
		assert.Equal(t, "user2", usersJob.Targets[0].User)
		assert.Equal(t, "pass2", usersJob.Targets[0].Password)
		assert.Equal(t, 2, usersJob.Targets[0].Port)
		assert.Equal(t, "host2_db", usersJob.Targets[0].DB)

		// Host3 target should get its values from defaults.targets[1], not defaults.hosts.host3
		assert.Equal(t, "host3", usersJob.Targets[1].Host)
		assert.Equal(t, "host3_label", usersJob.Targets[1].Label)
		assert.Equal(t, "host3_driver", usersJob.Targets[1].Driver)
		assert.Equal(t, "host3_user", usersJob.Targets[1].User)
		assert.Equal(t, "host3_pass", usersJob.Targets[1].Password)
		assert.Equal(t, 3, usersJob.Targets[1].Port)
		assert.Equal(t, "host3_db", usersJob.Targets[1].DB)

		// Pets job should not get default source host (because it has a DSN)
		assert.Empty(t, petsJob.Source.Host)

		// Pets job should get default targets
		require.Len(t, petsJob.Targets, 2)

		// Each target should NOT get default table from source, since source is given by DSN
		for _, target := range petsJob.Targets {
			assert.Empty(t, target.Table)
		}

		// Posts job should get overridden source host
		assert.Equal(t, "host1_override", postsJob.Source.Host)

		// Posts job should get overridden targets
		require.Len(t, postsJob.Targets, 2)

		assert.Equal(t, "posts_dsn2", postsJob.Targets[0].DSN)
		assert.Equal(t, "posts_dsn3", postsJob.Targets[1].DSN)
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
			description: "missing source table (with label)",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Source.Table = ""
				cfg.Source.Label = "foobarbaz"
				return cfg
			},
			expectedErr: `"foobarbaz": table name is empty`,
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
			description: "missing source driver (with label)",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Source.Driver = ""
				cfg.Source.Label = "foobarbaz"
				return cfg
			},
			expectedErr: `"foobarbaz": table does not specify a driver`,
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
			description: "missing target table (with label)",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets[0].Table = ""
				cfg.Targets[0].Label = "foobarbaz"
				return cfg
			},
			expectedErr: `"foobarbaz": table name is empty`,
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
		{
			description: "missing target driver (with label)",
			job: func() JobConfig {
				cfg := validJob()
				cfg.Targets[0].Driver = ""
				cfg.Targets[0].Label = "foobarbaz"
				return cfg
			},
			expectedErr: `"foobarbaz": table does not specify a driver`,
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
