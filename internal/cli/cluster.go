package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/syleron/pulseha/internal/client"
	"github.com/syleron/pulseha/rpc"
)

func NewClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage cluster operations",
		Long:  `Perform cluster-wide operations such as creating, joining, or leaving a cluster`,
	}

	cmd.AddCommand(
		newClusterCreateCmd(),
		newClusterJoinCmd(),
		newClusterLeaveCmd(),
		newClusterTokenCmd(),
		newClusterModeCmd(),
	)

	return cmd
}

func newClusterCreateCmd() *cobra.Command {
	var (
		bindIP   string
		bindPort string
		mode     string
	)

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new cluster",
		RunE:  createCluster,
	}

	cmd.Flags().StringVar(&bindIP, "bind-ip", "", "IP address to bind to")
	cmd.Flags().StringVar(&bindPort, "bind-port", "8080", "Port to bind to")
	cmd.Flags().StringVar(&mode, "mode", "active-passive", "Cluster mode (active-passive or active-active)")
	cmd.MarkFlagRequired("bind-ip")

	return cmd
}

func newClusterJoinCmd() *cobra.Command {
	var (
		address  string
		token    string
		bindIP   string
		bindPort string
	)

	cmd := &cobra.Command{
		Use:   "join",
		Short: "Join an existing cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := client.New()
			if err != nil {
				return err
			}

			return client.JoinCluster(address, token, bindIP, bindPort)
		},
	}

	cmd.Flags().StringVar(&address, "address", "", "Address of an existing cluster member (FQDN or IP:port)")
	cmd.Flags().StringVar(&token, "token", "", "Cluster join token")
	cmd.Flags().StringVar(&bindIP, "bind-ip", "", "Local IP address to bind to (optional)")
	cmd.Flags().StringVar(&bindPort, "bind-port", "", "Local port to bind to (optional)")
	cmd.MarkFlagRequired("address")
	cmd.MarkFlagRequired("token")

	return cmd
}

func newClusterLeaveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "leave",
		Short: "Leave the current cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := client.New()
			if err != nil {
				return err
			}

			return client.LeaveCluster()
		},
	}

	return cmd
}

func newClusterTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "Display cluster join token",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := client.New()
			if err != nil {
				return fmt.Errorf("failed to connect to PulseHA daemon - ensure the pulseha service is running: %v", err)
			}
			defer client.Close()

			// Get cluster status to read token from config
			cfg := client.GetConfig()
			if !cfg.ClusterCheck() {
				return fmt.Errorf("no cluster configured")
			}

			if cfg.Pulse.ClusterToken == "" {
				return fmt.Errorf("no cluster token available")
			}

			fmt.Println(cfg.Pulse.ClusterToken)
			return nil
		},
	}

	return cmd
}

func newClusterModeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mode",
		Short: "Manage cluster operation mode",
	}

	cmd.AddCommand(newClusterModeSetCmd())
	return cmd
}

func newClusterModeSetCmd() *cobra.Command {
	var mode string

	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set cluster operation mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := client.New()
			if err != nil {
				return err
			}

			// Validate mode
			switch mode {
			case "active-passive", "active-active":
				// Valid modes
			default:
				return fmt.Errorf("invalid mode %q: must be either 'active-passive' or 'active-active'", mode)
			}

			return client.SetClusterMode(mode)
		},
	}

	cmd.Flags().StringVar(&mode, "mode", "", "Cluster mode (active-passive or active-active)")
	cmd.MarkFlagRequired("mode")

	return cmd
}

// createCluster creates a new cluster
func createCluster(cmd *cobra.Command, args []string) error {
	// Get bind IP and port
	bindIP, _ := cmd.Flags().GetString("bind-ip")
	bindPort, _ := cmd.Flags().GetString("bind-port")
	mode, _ := cmd.Flags().GetString("mode")

	// Validate mode
	if mode != "active-passive" && mode != "active-active" {
		return fmt.Errorf("invalid mode %q: must be either 'active-passive' or 'active-active'", mode)
	}

	// Create client - this will try to connect to localhost:8080 by default
	c, err := client.New()
	if err != nil {
		return fmt.Errorf("failed to connect to PulseHA daemon - ensure the pulseha service is running: %v", err)
	}
	defer c.Close()

	// Send create cluster request
	resp, err := c.CLI().CreateCluster(context.Background(), &rpc.CreateClusterRequest{
		BindIp:   bindIP,
		BindPort: bindPort,
		Mode:     mode,
	})
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf(resp.Message)
	}

	fmt.Println("Cluster created successfully!")

	// Display the cluster token if it was returned
	if resp.Token != "" {
		fmt.Println("\nCluster join token:")
		fmt.Println(resp.Token)
		fmt.Println("\nUse this token when joining other nodes to the cluster.")
	}

	return nil
}
