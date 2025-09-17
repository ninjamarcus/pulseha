package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/syleron/pulseha/internal/client"
	rpc "github.com/syleron/pulseha/rpc"
)

func NewNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Manage cluster nodes",
		Long:  `Perform operations on cluster nodes such as promoting, demoting, or removing nodes`,
	}

	cmd.AddCommand(
		newNodePromoteCmd(),
		newNodeDemoteCmd(),
		newNodeRemoveCmd(),
	)

	return cmd
}

func newNodePromoteCmd() *cobra.Command {
	var nodeID string
	var ips []string

	cmd := &cobra.Command{
		Use:   "promote",
		Short: "Promote a node to active state",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.New()
			if err != nil {
				return err
			}
			defer c.Close()

			if nodeID == "" {
				return fmt.Errorf("--node-id is required")
			}

			resp, err := c.CLI().Promote(context.Background(), &rpc.PromoteRequest{
				NodeId: nodeID,
				Ips:    ips,
			})
			if err != nil {
				return err
			}
			if !resp.Success {
				return fmt.Errorf(resp.Message)
			}
			fmt.Println(resp.Message)
			return nil
		},
	}

	cmd.Flags().StringVar(&nodeID, "node-id", "", "Node ID (UUID) of the node to promote")
	cmd.Flags().StringSliceVar(&ips, "ips", []string{}, "IPs to assign in active-active mode")
	cmd.MarkFlagRequired("node-id")

	return cmd
}

func newNodeDemoteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "demote",
		Short: "Demote a node to passive state",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement node demotion
			return fmt.Errorf("node demotion not implemented yet")
		},
	}
	return cmd
}

func newNodeRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove a node from the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement node removal
			return fmt.Errorf("node removal not implemented yet")
		},
	}
	return cmd
}
