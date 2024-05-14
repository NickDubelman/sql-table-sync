package sync

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config contains the sync jobs and any other configuration for the sync process
type Config struct {
	Defaults ConfigDefaults
	Jobs     map[string]JobConfig
}

type ConfigDefaults struct {
	Driver string
	Hosts  map[string]CredentialsConfig
}

// JobConfig contains the configuration for a single sync job
type JobConfig struct {
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

type CredentialsConfig struct {
	Driver   string
	DSN      string
	User     string
	Password string
	Port     int
	DB       string
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

	// Impose some default values
	for jobName := range config.Jobs {
		job := config.Jobs[jobName]

		// For each job, if PrimaryKey is empty, set it to "id"
		if job.PrimaryKey == "" && len(job.PrimaryKeys) == 0 {
			job.PrimaryKey = "id"
		}

		// If PrimaryKey is non-empty, copy it to PrimaryKeys
		if job.PrimaryKey != "" {
			job.PrimaryKeys = []string{job.PrimaryKey}
		}

		// If host is given, check to see if there is an entry in the credential map
		job.Source = imposeDefaultCredentials(job.Source, config.Defaults)

		for j := range job.Targets {
			job.Targets[j] = imposeDefaultCredentials(job.Targets[j], config.Defaults)
		}

		config.Jobs[jobName] = job // Update the map
	}

	return config, nil
}

func (c Config) validate() error {
	// Make sure there is at least one job
	if len(c.Jobs) == 0 {
		return fmt.Errorf("no jobs found in config")
	}

	for name, job := range c.Jobs {
		// Make sure every job has a non-empty name
		if name == "" {
			return fmt.Errorf("all jobs must have a name")
		}

		if err := job.validate(); err != nil {
			return fmt.Errorf("job '%s' %w", name, err)
		}
	}

	return nil
}

func (cfg JobConfig) validate() error {
	// Make sure primaryKeys is populated
	if len(cfg.PrimaryKeys) == 0 {
		return fmt.Errorf("has no primary keys")
	}

	// Make sure primaryKeys has length <= 3
	if len(cfg.PrimaryKeys) > 3 {
		return fmt.Errorf("has too many primary keys")
	}

	// Make sure columns is non-empty
	if len(cfg.Columns) == 0 {
		return fmt.Errorf("does not specify any columns")
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
			return fmt.Errorf("has primary key '%s' not in columns", key)
		}
	}

	// Make sure every job has a non-empty source table
	if err := cfg.Source.validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}

	// Make sure every job has at least one target
	if len(cfg.Targets) == 0 {
		return fmt.Errorf("has no targets")
	}

	for i, target := range cfg.Targets {
		if err := target.validate(); err != nil {
			return fmt.Errorf("target[%d]: %w", i, err)
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

func imposeDefaultCredentials(
	table TableConfig,
	defaults ConfigDefaults,
) TableConfig {
	var hostDefaults CredentialsConfig
	if table.Host != "" {
		hostDefaults = defaults.Hosts[table.Host]
	}

	// If Driver is not empty, set it to the default driver
	if table.Driver == "" {
		if hostDefaults.Driver != "" {
			table.Driver = hostDefaults.Driver // Default from the credentials for the host
		} else {
			table.Driver = defaults.Driver // Global default driver
		}
	}

	if table.DSN == "" {
		// If DSN is not provided, default to the DSN from the credential map
		table.DSN = hostDefaults.DSN
	}

	if table.User == "" {
		// If User is not provided, default to the User from the credential map
		table.User = hostDefaults.User
	}

	if table.Password == "" {
		// If Password is not provided, default to the Password from the credential map
		table.Password = hostDefaults.Password
	}

	if table.Port == 0 {
		// If Port is not provided, default to the Port from the credential map
		table.Port = hostDefaults.Port
	}

	if table.DB == "" {
		// If DB is not provided, default to the DB from the credential map
		table.DB = hostDefaults.DB
	}

	return table
}
