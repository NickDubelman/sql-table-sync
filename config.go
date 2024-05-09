package sync

import (
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

// Config contains the sync jobs and any other configuration for the sync process
type Config struct {
	Driver string
	Jobs   []JobConfig
}

// JobConfig contains the configuration for a single sync job
type JobConfig struct {
	// Name uniquely identifies a job
	Name string

	// Columns defines the columns for the source and target tables
	Columns []string

	// Can either specify one primary key or multiple primary key columns
	// If neither is specified, the default is "id"
	// The primary key(s) must be a subset of Columns

	// PrimaryKey is the name of a single primary key column
	PrimaryKey string `yaml:"primaryKey"`

	// PrimaryKeys is a list of composite primary key columns
	PrimaryKeys []string `yaml:"primaryKeys"`

	Source  TableConfig
	Targets []TableConfig
}

// TableConfig contains the configuration for a single table (source or target)
type TableConfig struct {
	// Label is an optional human-readable name for the table
	Label string

	// Table is the name of the table
	Table string

	// Driver is the database driver to use. For now, only sqlite3 and mysql are supported
	Driver string

	// DSN overrides any other connection parameters
	DSN string

	// If DSN is not explicitly provided, it will be inferred from the below parameters

	User     string
	Password string
	Host     string
	Port     int
	DB       string
}

// LoadConfig reads a config file and makes sure it is valid
func LoadConfig(filename string) (Config, error) {
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	config, err := loadConfig(string(fileBytes))
	if err != nil {
		return Config{}, err
	}

	if err := config.validate(); err != nil {
		return Config{}, err
	}

	return config, nil
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

func (c Config) validate() error {
	// Make sure there is at least one job
	if len(c.Jobs) == 0 {
		return fmt.Errorf("no jobs found in config")
	}

	// Make sure each job name is unique
	jobNames := map[string]int{} // name -> count

	for _, job := range c.Jobs {
		if err := job.validate(); err != nil {
			return err
		}

		jobNames[job.Name]++
	}

	var duplicateNames []string
	for name, count := range jobNames {
		if count > 1 {
			duplicateNames = append(duplicateNames, name)
		}
	}
	slices.Sort(duplicateNames) // Sort deterministically (alphabetically)

	if len(duplicateNames) > 0 {
		return fmt.Errorf("duplicate job names: %v", duplicateNames)
	}

	return nil
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
