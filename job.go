package sync

import (
	"fmt"
	"log"
)

func (c Config) ExecJob(jobName string) error {
	// Find the job with the given name
	var job JobConfig
	for _, j := range c.Jobs {
		if j.Name == jobName {
			job = j
			break
		}
	}

	// If no matching job was found, return an error
	if job.Name == "" {
		return fmt.Errorf("job '%s' not found in config", jobName)
	}

	// Connect to source
	source, err := Connect(job.Source)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}

	// Attempt to connect to each target
	targets := make([]Table, len(job.Targets))
	for i, target := range job.Targets {
		table, err := Connect(target)
		if err != nil {
			log.Printf("failed to connect to target[%d]: %v", i, err)
		}

		targets[i] = table
	}

	return sync(job.PrimaryKey, job.Columns, source, targets)
}
