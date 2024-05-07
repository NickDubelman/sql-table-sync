package sync

import (
	"fmt"
	"os"

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

func (c Config) Ping() error {
	return nil // TODO:
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
