package main

import (
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/spf13/cobra"

	sync "github.com/NickDubelman/sql-table-sync"
)

var pingTimeoutStr string

func init() {
	rootCmd.AddCommand(pingCmd)
	pingCmd.Flags().StringVarP(
		&pingTimeoutStr, "timeout", "t", "10s", "timeout for pinging each table",
	)
}

var pingCmd = &cobra.Command{
	Use:   "ping [job]...",
	Short: "Pings the given sync jobs",
	Long:  "Pings the given sync jobs to see which databases are reachable. If no positional args are provided, pings all jobs.",
	Run: func(cmd *cobra.Command, args []string) {
		timeout, err := time.ParseDuration(pingTimeoutStr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		if len(args) == 0 {
			allResults, err := config.PingAllJobs(timeout)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			var jobNames []string
			for jobName := range config.Jobs {
				jobNames = append(jobNames, jobName)
			}
			slices.Sort(jobNames) // Sort the job names so the output is deterministic

			for i, jobName := range jobNames {
				if i != 0 {
					fmt.Println() // Add a newline between job results
				}

				printPingOutput(jobName, allResults[jobName], nil)
			}
		} else {
			for i, jobName := range args {
				if i != 0 {
					fmt.Println() // Add a newline between job results
				}

				results, err := config.PingJob(jobName, timeout)
				printPingOutput(jobName, results, err)
			}
		}
	},
}

func printPingOutput(jobName string, results []sync.PingResult, err error) {
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(jobName + ":")

	var numOk int
	var tableErrs []string

	for _, r := range results {
		if r.Error != nil {
			errStr := fmt.Sprintf("%s: %s", r.Config.Label, r.Error)
			tableErrs = append(tableErrs, errStr)
		} else {
			numOk++
		}
	}

	var resultStr string
	if numOk == len(results) {
		resultStr = "all ok"
	} else {
		resultStr = fmt.Sprintf("%d ok", numOk)
	}

	if len(tableErrs) > 0 {
		resultStr += fmt.Sprintf(", %d errored", len(tableErrs))
	}

	fmt.Println("  - tables:", resultStr)

	if len(tableErrs) > 0 {
		for _, err := range tableErrs {
			fmt.Println("    -", err)
		}
	}
}
