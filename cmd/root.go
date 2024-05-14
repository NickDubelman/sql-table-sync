package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	sync "github.com/NickDubelman/sql-table-sync"
)

var configFilename string
var config sync.Config

func init() {
	cobra.OnInitialize(func() {
		var err error
		config, err = sync.LoadConfig(configFilename)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	})

	rootCmd.PersistentFlags().StringVarP(
		&configFilename, "config", "c", "./sync-config.yaml", "config file",
	)
}

var rootCmd = &cobra.Command{
	Use:   "sql-table-sync",
	Short: "Sync SQL tables between databases",
}

func executeRootCmd() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
