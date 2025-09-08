package cli

import (\n\t"encoding/json"\n\t"fmt"\n\t"context"\n\t"time"\n\n\t"github.com/spf13/cobra"\n\t"github.com/syleron/pulseha/internal/client"\n\trpc "github.com/syleron/pulseha/rpc"\n)

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

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := client.CLI().Status(ctx, &rpc.StatusRequest{})
			if err != nil {
				return err
			}

			// Translate RPC to client.ClusterStatus
			status, convErr := translateStatusResponse(resp)
			if convErr != nil {
				return convErr
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

func translateStatusResponse(resp *rpc.StatusResponse) (*client.ClusterStatus, error) {
	cfg := client.New().GetConfig()
	status := &client.ClusterStatus{
		Members: make([]client.Member, len(resp.Members)),
		Groups:  make([]client.GroupInfo, 0),
		Mode:    cfg.Pulse.Mode,
	}
	for i, m := range resp.Members {
		s := "Unknown"
		switch m.Status {
		case rpc.MemberStatusEnum_MEMBER_STATUS_ACTIVE:
			s = "Active"
		case rpc.MemberStatusEnum_MEMBER_STATUS_PASSIVE:
			s = "Passive"
		case rpc.MemberStatusEnum_MEMBER_STATUS_PARTIAL_ACTIVE:
			s = "PartialActive"
		}
		status.Members[i] = client.Member{
			Hostname:      m.Hostname,
			IP:            m.Ip,
			Port:          m.Port,
			Status:        s,
			IPs:           m.ActiveIps,
			ActiveIPs:     m.ActiveIps,
			LastResponse:  m.LastResponse,
			Latency:       m.Latency,
			PartialActive: m.PartialActive,
		}
	}
	for groupName, ips := range cfg.Groups {
		gi := client.GroupInfo{Name: groupName, IPs: ips}
		for id, node := range cfg.Nodes {
			for iface, groups := range node.IPGroups {
				for _, g := range groups {
					if g == groupName {
						gi.Assignments = append(gi.Assignments, client.GroupAssignment{NodeID: id, Interface: iface})
					}
				}
			}
		}
		status.Groups = append(status.Groups, gi)
	}
	return status, nil
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
					fmt.Printf("    - Node: %s, Interface: %s\n", assignment.NodeID, assignment.Interface)
				}
			} else {
				fmt.Printf("  Assigned to: None\n")
			}
		}
	}

	return nil
}
