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
	job, ok := c.Jobs[jobName]
	if !ok {
		return ExecJobResult{}, fmt.Errorf("job '%s' not found in config", jobName)
	}

	checksum, results, err := job.syncTargets()
	return ExecJobResult{checksum, results}, err
}

// ExecAllJobs executes all jobs in the sync config
func (c Config) ExecAllJobs() (map[string]ExecJobResult, map[string]error) {
	results := make(map[string]ExecJobResult, len(c.Jobs))
	errors := make(map[string]error, len(c.Jobs))

	for jobName := range c.Jobs {
		result, err := c.ExecJob(jobName)
		results[jobName] = result
		errors[jobName] = err
	}

	return results, errors
}
