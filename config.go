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

	// Source is the default source to use if a job does not specify one
	Source *SourceTargetDefault

	// Targets are the default targets to use if a job does not specify any. This can only be used
	// if each target has the same table as the source
	Targets []SourceTargetDefault
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

// SourceTargetDefault contains the default values for a source or target table
type SourceTargetDefault struct {
	DSN      string
	Host     string
	Label    string
	Driver   string
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

		// Impose default credentials on the source
		sourceHasDSN := job.Source.DSN != ""
		sourceHasHost := job.Source.Host != ""

		// If source does not have DSN or Host, and a default source is provided, apply the values
		if !sourceHasDSN && !sourceHasHost && config.Defaults.Source != nil {
			if job.Source.Label == "" {
				job.Source.Label = config.Defaults.Source.Label
			}

			if job.Source.Driver == "" {
				job.Source.Driver = config.Defaults.Source.Driver
			}

			if job.Source.DSN == "" {
				job.Source.DSN = config.Defaults.Source.DSN
			}

			if job.Source.User == "" {
				job.Source.User = config.Defaults.Source.User
			}

			if job.Source.Password == "" {
				job.Source.Password = config.Defaults.Source.Password
			}

			if job.Source.Host == "" {
				job.Source.Host = config.Defaults.Source.Host
			}

			if job.Source.Port == 0 {
				job.Source.Port = config.Defaults.Source.Port
			}

			if job.Source.DB == "" {
				job.Source.DB = config.Defaults.Source.DB
			}
		}

		job.Source = imposeTableDefaults(job.Source, config.Defaults)

		// If there are no targets, initialize a list of targets with the default target hosts
		if len(job.Targets) == 0 {
			for _, targetHost := range config.Defaults.Targets {
				job.Targets = append(job.Targets, TableConfig{
					Label:    targetHost.Label,
					Driver:   targetHost.Driver,
					DSN:      targetHost.DSN,
					User:     targetHost.User,
					Password: targetHost.Password,
					Host:     targetHost.Host,
					Port:     targetHost.Port,
					DB:       targetHost.DB,
				})
			}
		}

		// Impose default credentials on each target
		for j := range job.Targets {
			job.Targets[j] = imposeTableDefaults(job.Targets[j], config.Defaults)

			sourceHasDSN := job.Source.DSN != ""
			sourceHasHost := job.Source.Host != ""
			targetHasDSN := job.Targets[j].DSN != ""
			targetHasHost := job.Targets[j].Host != ""
			hasDifferentDSN := job.Source.DSN != job.Targets[j].DSN
			hasDifferentHost := job.Source.Host != job.Targets[j].Host

			if sourceHasDSN && targetHasDSN && hasDifferentDSN {
				// If the source and target both have DSNs and they are different, default target
				// table to same as source table
				if job.Targets[j].Table == "" {
					job.Targets[j].Table = job.Source.Table
				}
			} else if sourceHasHost && targetHasHost && hasDifferentHost {
				// If the source and target both have hosts and they are different, default target
				// table to same as source table
				if job.Targets[j].Table == "" {
					job.Targets[j].Table = job.Source.Table
				}
			}
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

func imposeTableDefaults(table TableConfig, defaults ConfigDefaults) TableConfig {
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
