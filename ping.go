package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
)

type PingResult struct {
	Results []JobPingResult
}

type JobPingResult struct {
	Job     JobConfig
	Results []TablePingResult
}

type TablePingResult struct {
	TableConfig TableConfig
	Error       error
}

// Ping checks all jobs in the config to ensure that each source and target table:
//   - is reachable
//   - has the correct credentials
//   - exists
//   - has the expected columns
func (c Config) Ping() (PingResult, error) {
	timeout := 30 * time.Second

	// Iterate over all jobs and "ping" the source and targets
	jobResults := make([]JobPingResult, len(c.Jobs))

	for i, job := range c.Jobs {
		jobResults[i].Job = job

		// Ping the source table
		jobResults[i].Results = append(jobResults[i].Results, TablePingResult{
			TableConfig: job.Source,
			Error:       pingWithTimeout(timeout, job.Source, job.Columns),
		})

		// Ping the target tables (in parallel)
		var wg sync.WaitGroup
		resultChan := make(chan TablePingResult, len(job.Targets))

		for _, target := range job.Targets {
			wg.Add(1)
			go func(target TableConfig) {
				defer wg.Done()

				resultChan <- TablePingResult{
					TableConfig: target,
					Error:       pingWithTimeout(timeout, target, job.Columns),
				}
			}(target)
		}

		wg.Wait()         // Wait for all goroutines to finish
		close(resultChan) // Close the channel to signal that all results have been sent

		// Collect the results from the channel
		for result := range resultChan {
			jobResults[i].Results = append(jobResults[i].Results, result)
		}
	}

	return PingResult{Results: jobResults}, nil
}

// Ping the source and targets for a given TableConfig
func ping(config TableConfig, columns []string) error {
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

	rows, err := t.Query(sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return nil
}

// Ping the source and targets with a timeout
func pingWithTimeout(timeout time.Duration, config TableConfig, columns []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create a channel to receive the ping result
	resultChan := make(chan error, 1)

	go func() {
		resultChan <- ping(config, columns)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("ping operation timed out") // Timeout exceeded
	case err := <-resultChan:
		return err // Ping operation completed, return the result
	}
}
