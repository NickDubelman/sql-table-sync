package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
)

// PingResult contains the results of pinging a single table
type PingResult struct {
	Config TableConfig
	Error  error
}

// PingJob checks a single job in the config to ensure that each source and target table:
//   - is reachable
//   - has the correct credentials
//   - exists
//   - has the expected columns
func (c Config) PingJob(jobName string, timeout time.Duration) ([]PingResult, error) {
	// Find the job with the given name
	job, ok := c.Jobs[jobName]
	if !ok {
		return nil, fmt.Errorf("job '%s' not found in config", jobName)
	}

	var results []PingResult

	// Ping the source table
	sourceLabel := job.Source.Label
	if sourceLabel == "" {
		sourceLabel = "source"
	}

	results = append(results, PingResult{
		Config: job.Source,
		Error:  pingWithTimeout(timeout, job.Source, job.Columns),
	})

	// Ping the target tables (in parallel)
	var wg sync.WaitGroup
	resultChan := make(chan PingResult, len(job.Targets))

	for j, target := range job.Targets {
		wg.Add(1)
		go func(j int, target TableConfig) {
			defer wg.Done()

			resultChan <- PingResult{
				Config: target,
				Error:  pingWithTimeout(timeout, target, job.Columns),
			}
		}(j, target)
	}

	wg.Wait()         // Wait for all goroutines to finish
	close(resultChan) // Close the channel to signal that all results have been sent

	// Collect the results from the channel
	for result := range resultChan {
		results = append(results, result)
	}

	return results, nil
}

// PingAllJobs checks all jobs in the config to ensure that each source and target table:
//   - is reachable
//   - has the correct credentials
//   - exists
//   - has the expected columns
func (c Config) PingAllJobs(timeout time.Duration) (map[string][]PingResult, error) {
	// Iterate over all jobs and "ping" the source and targets
	results := make(map[string][]PingResult, len(c.Jobs))

	for jobName := range c.Jobs {
		jobResults, err := c.PingJob(jobName, timeout)
		if err != nil {
			// This can't actually happen because the only way for PingJob to error is if the job
			// doesn't exist (but we are iterating on the jobs)
			return nil, err
		}

		results[jobName] = jobResults
	}

	return results, nil
}

// Ping the source and targets with a timeout
func pingWithTimeout(timeout time.Duration, config pingTarget, columns []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create a channel to receive the ping result
	resultChan := make(chan error, 1)

	go func() {
		resultChan <- config.ping(columns)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("ping operation timed out") // Timeout exceeded
	case err := <-resultChan:
		return err // Ping operation completed, return the result
	}
}

type pingTarget interface {
	ping(columns []string) error
}

// Ping the source and targets for a given TableConfig
func (config TableConfig) ping(columns []string) error {
	t := table{config: config}
	if err := t.connect(); err != nil {
		return err
	}
	defer t.Close()

	// Make sure we can query the table
	query := sq.Select(columns...).From(config.Table).Limit(1)
	sql, args, err := query.ToSql()
	if err != nil {
		return err
	}

	rows, err := t.Queryx(sql, args...)
	if err != nil {
		return err
	}

	return rows.Close()
}
