package main

import (
	"fmt"
	"slices"

	"github.com/spf13/cobra"

	sync "github.com/NickDubelman/sql-table-sync"
)

func init() {
	rootCmd.AddCommand(execCmd)
}

var execCmd = &cobra.Command{
	Use:   "exec [job]...",
	Short: "Execute the given sync jobs",
	Long:  `Execute the given sync jobs. If no positional args are provided, executes all jobs.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			results, errs := config.ExecAllJobs()

			var jobNames []string
			for jobName := range config.Jobs {
				jobNames = append(jobNames, jobName)
			}
			slices.Sort(jobNames) // Sort the job names so the output is deterministic

			for i, jobName := range jobNames {
				if i != 0 {
					fmt.Println() // Add a newline between job results
				}

				printExecOutput(jobName, results[jobName], errs[jobName])
			}
		} else {
			for i, jobName := range args {
				if i != 0 {
					fmt.Println() // Add a newline between job results
				}

				result, err := config.ExecJob(jobName)
				printExecOutput(jobName, result, err)
			}
		}
	},
}

func printExecOutput(jobName string, result sync.ExecJobResult, err error) {
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(jobName + ":")
	fmt.Println("  - source checksum:", result.Checksum)

	var numOk, numChanged int
	var targetErrs []string

	for _, r := range result.Results {
		if r.Error != nil {
			errStr := fmt.Sprintf("%s: %s", r.Target.Label, r.Error)
			targetErrs = append(targetErrs, errStr)
		} else {
			numOk++

			if r.Synced {
				numChanged++
			}
		}
	}

	resultStr := fmt.Sprintf("%d ok, %d changed", numOk, numChanged)
	if len(targetErrs) > 0 {
		resultStr += fmt.Sprintf(", %d errored", len(targetErrs))
	}

	fmt.Println("  - targets:", resultStr)

	if len(targetErrs) > 0 {
		for _, err := range targetErrs {
			fmt.Println("    -", err)
		}
	}
}
