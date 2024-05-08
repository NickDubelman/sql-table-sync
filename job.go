package sync

import "fmt"

func (c Config) ExecJob(jobName string) (string, []SyncResult, error) {
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
		return "", nil, fmt.Errorf("job '%s' not found in config", jobName)
	}

	primaryKeyIndices := job.getPrimaryKeyIndices()

	source := table{
		config:            job.Source,
		primaryKeys:       job.PrimaryKeys,
		primaryKeyIndices: primaryKeyIndices,
		columns:           job.Columns,
	}

	targets := make([]table, len(job.Targets))
	for i, target := range job.Targets {
		targets[i] = table{
			config:            target,
			primaryKeys:       job.PrimaryKeys,
			primaryKeyIndices: primaryKeyIndices,
			columns:           job.Columns,
		}
	}

	return syncTargets(source, targets)
}
