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
	Name       string
	Columns    []string
	PrimaryKey string `yaml:"primaryKey"`
	Source     TableConfig
	Targets    []TableConfig
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

	// Make sure every job has a non-empty source table
	for _, job := range c.Jobs {
		if job.Source.Table == "" {
			return fmt.Errorf("job %s has no source table", job.Name)
		}
	}

	// Make sure every job has at least one target
	for _, job := range c.Jobs {
		if len(job.Targets) == 0 {
			return fmt.Errorf("job %s has no targets", job.Name)
		}
	}

	// Make sure every target has a non-empty table
	for _, job := range c.Jobs {
		for i, target := range job.Targets {
			if target.Table == "" {
				return fmt.Errorf("job %s, target[%d] with no table", job.Name, i)
			}
		}
	}

	// TODO: what else makes sense to validate?

	return nil
}

// Ping checks all jobs in the config to ensure that each source and target table:
//   - is reachable
//   - has the correct credentials
//   - exists
//   - has the expected columns
func (c Config) Ping() error {
	// Iterate over all jobs and "ping" the source and targets
	ping := func(table TableConfig, columns []string) error {
		conn, err := Connect(table)
		if err != nil {
			return err
		}

		query := sq.Select(columns...).From(table.Table).Limit(1)
		sql, args, err := query.ToSql()
		if err != nil {
			return err
		}

		rows, err := conn.Query(sql, args...)
		if err != nil {
			return err
		}

		defer rows.Close()

		// Close the db connection, just to be safe
		if err := conn.Close(); err != nil {
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
		if config.Jobs[i].PrimaryKey == "" {
			config.Jobs[i].PrimaryKey = "id"
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
