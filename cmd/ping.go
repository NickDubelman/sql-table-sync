package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
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
			pingResults, err := config.PingAllJobs(30 * time.Second)
			if err != nil {
				fmt.Println(err)
			}

			for _, r := range pingResults {
				for _, result := range r.Tables {
					if result.Error != nil {
						fmt.Println(r.Job.Name, result.Label, result.Error)
					}
				}
			}
		} else {
			for _, jobName := range args {
				results, err := config.PingJob(jobName, 30*time.Second)
				if err != nil {
					fmt.Println(err)
				}

				for _, result := range results {
					if result.Error != nil {
						fmt.Println(jobName, result.Label, result.Error)
					}
				}
			}
		}
	},
}
