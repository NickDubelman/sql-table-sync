package sync

import (
	"fmt"
	"os"

	sq "github.com/Masterminds/squirrel"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Driver string
	Jobs   []JobConfig
}

type JobConfig struct {
	Name        string
	Columns     []string
	PrimaryKey  string   `yaml:"primaryKey"`
	PrimaryKeys []string `yaml:"primaryKeys"`
	Source      TableConfig
	Targets     []TableConfig
}

type TableConfig struct {
	Label    string
	Driver   string
	DSN      string
	User     string
	Password string
	Host     string
	Port     int
	DB       string
	Table    string
}

func LoadConfig(filename string) (Config, error) {
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	return loadConfig(string(fileBytes))
}

func (c Config) Validate() error {
	// Make sure there is at least one job
	if len(c.Jobs) == 0 {
		return fmt.Errorf("no jobs found in config")
	}

	for _, job := range c.Jobs {
		if err := job.validate(); err != nil {
			return err
		}
	}

	return nil
}

// Ping checks all jobs in the config to ensure that each source and target table:
//   - is reachable
//   - has the correct credentials
//   - exists
//   - has the expected columns
//
// TODO: instead of just erroring, return a result
// TODO: jobs in series, but tables in parallel (with timeout)
func (c Config) Ping() error {
	// Iterate over all jobs and "ping" the source and targets
	ping := func(config TableConfig, columns []string) error {
		t := table{config: config}
		if err := t.connect(); err != nil {
			return err
		}

		// Make sure we can query the table
		query := sq.Select(columns...).From(config.Table).Limit(1)
		sql, args, err := query.ToSql()
		if err != nil {
			return err
		}

		rows, err := t.Query(sql, args...)
		if err != nil {
			return err
		}

		defer rows.Close()

		// Close the db connection, just to be safe
		if err := t.Close(); err != nil {
			return err
		}

		return nil
	}

	for _, job := range c.Jobs {
		if err := ping(job.Source, job.Columns); err != nil {
			label := job.Source.Label
			if label == "" {
				label = "source"
			}
			return fmt.Errorf("job %s - %s, cannot ping: %w", job.Name, label, err)
		}

		for i, target := range job.Targets {
			if err := ping(target, job.Columns); err != nil {
				label := target.Label
				if label == "" {
					label = fmt.Sprintf("target[%d]", i)
				}
				return fmt.Errorf("job %s - %s, cannot ping: %w", job.Name, label, err)
			}
		}
	}

	return nil
}

func (c Config) ValidateAndPing() error {
	if err := c.Validate(); err != nil {
		return err
	}

	return c.Ping()
}

func loadConfig(fileContents string) (Config, error) {
	// Unmarshal fileContents into a Config struct
	var config Config

	if err := yaml.Unmarshal([]byte(fileContents), &config); err != nil {
		return Config{}, fmt.Errorf("failed to parse config: %w", err)
	}

	defaultDriver := config.Driver

	// Impose some default values
	for i := range config.Jobs {
		// For each job, if PrimaryKey is empty, set it to "id"
		if config.Jobs[i].PrimaryKey == "" && len(config.Jobs[i].PrimaryKeys) == 0 {
			config.Jobs[i].PrimaryKey = "id"
		}

		// If PrimaryKey is non-empty, copy it to PrimaryKeys
		if config.Jobs[i].PrimaryKey != "" {
			config.Jobs[i].PrimaryKeys = []string{config.Jobs[i].PrimaryKey}
		}

		// For each table, if User is empty, set it to "root"
		if config.Jobs[i].Source.User == "" {
			config.Jobs[i].Source.User = "root"
		}

		for j := range config.Jobs[i].Targets {
			if config.Jobs[i].Targets[j].User == "" {
				config.Jobs[i].Targets[j].User = "root"
			}
		}

		// For each table, if Driver is empty, set it to the default driver
		if config.Jobs[i].Source.Driver == "" {
			config.Jobs[i].Source.Driver = defaultDriver
		}

		for j := range config.Jobs[i].Targets {
			if config.Jobs[i].Targets[j].Driver == "" {
				config.Jobs[i].Targets[j].Driver = defaultDriver
			}
		}
	}

	return config, nil
}

func (cfg JobConfig) validate() error {
	// Make sure every job has a non-empty name
	if cfg.Name == "" {
		return fmt.Errorf("job has no name")
	}

	// Make sure primaryKeys is populated
	if len(cfg.PrimaryKeys) == 0 {
		return fmt.Errorf("job '%s' has no primary keys", cfg.Name)
	}

	// Make sure primaryKeys has length <= 3
	if len(cfg.PrimaryKeys) > 3 {
		return fmt.Errorf("job '%s' has too many primary keys", cfg.Name)
	}

	// Make sure columns is non-empty
	if len(cfg.Columns) == 0 {
		return fmt.Errorf("job '%s' does not specify any columns", cfg.Name)
	}

	// Make sure primaryKeys is a subset of columns
	for _, key := range cfg.PrimaryKeys {
		found := false
		for _, column := range cfg.Columns {
			if key == column {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("job '%s' has primary key '%s' not in columns", cfg.Name, key)
		}
	}

	// Make sure every job has a non-empty source table
	if err := cfg.Source.validate(); err != nil {
		return fmt.Errorf("job '%s' source: %w", cfg.Name, err)
	}

	// Make sure every job has at least one target
	if len(cfg.Targets) == 0 {
		return fmt.Errorf("job '%s' has no targets", cfg.Name)
	}

	for i, target := range cfg.Targets {
		if err := target.validate(); err != nil {
			return fmt.Errorf("job '%s' target[%d]: %w", cfg.Name, i, err)
		}
	}

	return nil
}

func (cfg TableConfig) validate() error {
	if cfg.Table == "" {
		return fmt.Errorf("table name is empty")
	}

	// Make sure source specifies a driver
	if cfg.Driver == "" {
		return fmt.Errorf("table does not specify a driver")
	}

	return nil
}
