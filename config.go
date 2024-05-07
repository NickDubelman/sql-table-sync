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
	return nil // TODO:
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
