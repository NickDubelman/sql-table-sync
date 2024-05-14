package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	sync "github.com/NickDubelman/sql-table-sync"
)

func init() {
	rootCmd.AddCommand(pingCmd)
}

var pingCmd = &cobra.Command{
	Use:   "ping [job]...",
	Short: "Pings the given sync jobs",
	Long:  "Pings the given sync jobs to see which databases are reachable. If no positional args are provided, pings all jobs.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			allResults, err := config.PingAllJobs(30 * time.Second)
			if err != nil {
				fmt.Println(err)
				return
			}

			for i, job := range config.Jobs {
				if i != 0 {
					fmt.Println() // Add a newline between job results
				}

				printPingOutput(job.Name, allResults[i].Tables, nil)
			}
		} else {
			for i, jobName := range args {
				if i != 0 {
					fmt.Println() // Add a newline between job results
				}

				results, err := config.PingJob(jobName, 30*time.Second)
				printPingOutput(jobName, results, err)
			}
		}
	},
}

func printPingOutput(jobName string, results []sync.TablePingResult, err error) {
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
