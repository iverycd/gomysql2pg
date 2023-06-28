package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gomysql2pg",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("\n\nyour version v0.1.3")
		os.Exit(0)
	},
}
