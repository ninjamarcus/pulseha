// PulseHA - HA Cluster Daemon
// Copyright (C) 2017-2021  Andrew Zak <andrew@linux.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/syleron/pulseha/internal/cli"
	"github.com/syleron/pulseha/packages/utils"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var (
	versionFlag bool
	rootCmd = &cobra.Command{
		Use:   "pulsectl",
		Short: "PulseHA cluster management tool",
		Long:  `PulseHA cluster management tool - Manage your high availability cluster`,
		Run: func(cmd *cobra.Command, args []string) {
			if versionFlag {
				fmt.Printf("PulseHA CLI Version: %s\n", utils.Version)
				fmt.Printf("Build: %s\n", utils.Build)
				os.Exit(0)
			}
			// If no flags or subcommands, show help
			cmd.Help()
		},
	}
)

func init() {
	rootCmd.Flags().BoolVarP(&versionFlag, "version", "v", false, "Show version information")
	
	rootCmd.AddCommand(
		cli.NewClusterCmd(),
		cli.NewNodeCmd(),
		cli.NewGroupCmd(),
		cli.NewStatusCmd(),
		cli.NewConfigCmd(),  // Add config command
	)
}
