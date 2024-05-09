package sync

import "fmt"

// ExecJobResult contains the results of executing a single sync job
type ExecJobResult struct {
	Checksum string
	Results  []SyncResult
}

// ExecJob executes a single job in the sync config
func (c Config) ExecJob(jobName string) (ExecJobResult, error) {
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
		return ExecJobResult{}, fmt.Errorf("job '%s' not found in config", jobName)
	}

	checksum, results, err := job.syncTargets()
	return ExecJobResult{checksum, results}, err
}

// ExecAllJobs executes all jobs in the sync config
func (c Config) ExecAllJobs() ([]ExecJobResult, []error) {
	results := make([]ExecJobResult, len(c.Jobs))
	errors := make([]error, len(c.Jobs))

	for i, job := range c.Jobs {
		result, err := c.ExecJob(job.Name)
		results[i] = result
		errors[i] = err
	}

	return results, errors
}
