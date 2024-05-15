package sync

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config contains the sync jobs and any other configuration for the sync process
type Config struct {
	// Defaults contains the user-specified defaults for the config
	Defaults ConfigDefaults

	// Jobs maps a set of job names to their definitions
	Jobs map[string]JobConfig
}

type ConfigDefaults struct {
	// Driver is the global default driver to use. For now, only sqlite3 and mysql are supported
	Driver string

	// Hosts maps hostnames to corresponding host-specific defaults
	Hosts map[string]HostDefaults
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

	// Source is the configuration for the source table (table to sync data from)
	Source TableConfig

	// Targets is a list of configurations for the target tables (tables to sync data to)
	Targets []TableConfig
}

// HostDefaults contains the host-specific default config values
type HostDefaults struct {
	Label    string
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
			return fmt.Errorf("job '%s': %w", name, err)
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
		label := "source"
		if cfg.Source.Label != "" {
			label = fmt.Sprintf(`"%s"`, cfg.Source.Label)
		}
		return fmt.Errorf("%s: %w", label, err)
	}

	// Make sure every job has at least one target
	if len(cfg.Targets) == 0 {
		return fmt.Errorf("has no targets")
	}

	for i, target := range cfg.Targets {
		label := fmt.Sprintf("target[%d]", i)
		if target.Label != "" {
			label = fmt.Sprintf(`"%s"`, target.Label)
		}

		if err := target.validate(); err != nil {
			return fmt.Errorf("%s: %w", label, err)
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

	// If DSN is given, make sure it is the only connection parameter
	if cfg.DSN != "" {
		if cfg.User != "" || cfg.Password != "" || cfg.Host != "" || cfg.Port != 0 || cfg.DB != "" {
			return fmt.Errorf("table cannot specify DSN and other connection parameters")
		}
	}

	return nil
}

func imposeDefaultCredentials(
	table TableConfig,
	defaults ConfigDefaults,
) TableConfig {
	var hostDefaults HostDefaults
	if table.Host != "" {
		hostDefaults = defaults.Hosts[table.Host]
	}

	// If Driver is empty, set it to either the global default or the host's defaults
	if table.Driver == "" {
		if hostDefaults.Driver != "" {
			table.Driver = hostDefaults.Driver // Host default
		} else {
			table.Driver = defaults.Driver // Global default
		}
	}

	// If DSN is empty, set it to the host's default
	if table.DSN == "" {
		table.DSN = hostDefaults.DSN
	}

	// If User is empty, set it to the host's default
	if table.User == "" {
		table.User = hostDefaults.User
	}

	// If Password is empty, set it to the host's default
	if table.Password == "" {
		table.Password = hostDefaults.Password
	}

	// If Port is empty, set it to the host's default
	if table.Port == 0 {
		table.Port = hostDefaults.Port
	}

	// If DB is empty, set it to the host's default
	if table.DB == "" {
		table.DB = hostDefaults.DB
	}

	// If Label is empty, set it to the host's default
	if table.Label == "" {
		table.Label = hostDefaults.Label
	}

	// If Label is still empty, default to DSN or Host:Port
	if table.Label == "" {
		if table.DSN != "" {
			table.Label = table.DSN
		} else if table.Host != "" && table.Port != 0 {
			table.Label = fmt.Sprintf("%s:%d", table.Host, table.Port)
		} else if table.Host != "" {
			table.Label = table.Host
		} else if table.Port != 0 {
			table.Label = fmt.Sprintf(":%d", table.Port)
		}
	}

	return table
}
