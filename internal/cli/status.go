package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/syleron/pulseha/internal/client"
)

func NewStatusCmd() *cobra.Command {
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := client.New()
			if err != nil {
				return err
			}

			status, err := client.GetClusterStatus()
			if err != nil {
				return err
			}

			if jsonOutput {
				output, err := json.MarshalIndent(status, "", "  ")
				if err != nil {
					return err
				}
				fmt.Println(string(output))
				return nil
			}

			// Pretty print status
			return printClusterStatus(status)
		},
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	return cmd
}

func printClusterStatus(status *client.ClusterStatus) error {
	fmt.Printf("\nCluster Status:\n")
	fmt.Printf("Mode: %s\n", status.Mode)
	fmt.Printf("==============\n")

	// Print node information
	fmt.Printf("\nNodes:\n")
	fmt.Printf("------\n")
	for _, member := range status.Members {
		fmt.Printf("\nNode: %s\n", member.Hostname)
		fmt.Printf("Address: %s:%s\n", member.IP, member.Port)
		fmt.Printf("Status: %s\n", member.Status)
		if len(member.ActiveIPs) > 0 {
			fmt.Printf("Active IPs: %v\n", member.ActiveIPs)
		}
		if member.PartialActive {
			fmt.Printf("Partially Active: Yes\n")
		}
		if member.LastResponse != "" {
			fmt.Printf("Last Response: %s\n", member.LastResponse)
		}
		if member.Latency != "" {
			fmt.Printf("Latency: %s\n", member.Latency)
		}
	}

	// Print group information
	if len(status.Groups) > 0 {
		fmt.Printf("\nFloating IP Groups:\n")
		fmt.Printf("------------------\n")

		for _, group := range status.Groups {
			fmt.Printf("\nGroup: %s\n", group.Name)

			// Print IPs in the group
			if len(group.IPs) > 0 {
				fmt.Printf("  IPs:\n")
				for _, ip := range group.IPs {
					fmt.Printf("    - %s\n", ip)
				}
			} else {
				fmt.Printf("  IPs: None\n")
			}

			// Print assignments
			if len(group.Assignments) > 0 {
				fmt.Printf("  Assigned to:\n")
				for _, assignment := range group.Assignments {
					fmt.Printf("    - Node: %s, Interface: %s\n", assignment.Hostname, assignment.Interface)
				}
			} else {
				fmt.Printf("  Assigned to: None\n")
			}
		}
	}

	return nil
}
