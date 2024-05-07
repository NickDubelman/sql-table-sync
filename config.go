package sync

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Jobs []JobConfig
}

type JobConfig struct {
	Name       string
	Columns    []string
	PrimaryKey string `yaml:"primaryKey"`
	Source     TableConfig
	Targets    []TableConfig
}

type TableConfig struct {
	Driver string
	DSN    string
	Host   string
	Port   int
	DB     string
	Table  string
}

func LoadConfig(filename string) (Config, error) {
	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	return loadConfig(string(fileBytes))
}

func loadConfig(fileContents string) (Config, error) {
	// Unmarshal fileContents into a Config struct
	var config Config

	if err := yaml.Unmarshal([]byte(fileContents), &config); err != nil {
		return Config{}, fmt.Errorf("failed to parse config: %w", err)
	}

	// Impose some default values
	for i := range config.Jobs {
		// For each job, if PrimaryKey is empty, set it to "id"
		if config.Jobs[i].PrimaryKey == "" {
			config.Jobs[i].PrimaryKey = "id"
		}
	}

	return config, nil
}
