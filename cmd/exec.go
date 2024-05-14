package main

import (
	"fmt"

	"github.com/spf13/cobra"
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
			for _, err := range errs {
				if err != nil {
					fmt.Println(err)
				}
			}

			for i, result := range results {
				jobName := config.Jobs[i].Name
				sourceChecksum := result.Checksum
				for _, result := range result.Results {
					if result.Error != nil {
						fmt.Println(jobName, result.Target, result.Error)
					} else {
						fmt.Println(jobName, result.Target, sourceChecksum, result.TargetChecksum)
					}
				}
			}
		} else {
			for _, jobName := range args {
				result, err := config.ExecJob(jobName)
				if err != nil {
					fmt.Println(err)
				}

				sourceChecksum := result.Checksum
				for _, r := range result.Results {
					if r.Error != nil {
						fmt.Println(jobName, r.Target, r.Error)
					} else {
						fmt.Println(jobName, r.Target, sourceChecksum, r.TargetChecksum)
					}
				}
			}
		}
	},
}
