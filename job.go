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

	source := Table{Config: job.Source}

	targets := make([]Table, len(job.Targets))
	for i, target := range job.Targets {
		targets[i] = Table{Config: target}
	}

	return syncTargets(source, targets, job.PrimaryKeys, job.Columns)
}
