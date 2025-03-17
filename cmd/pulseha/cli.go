package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/syleron/pulseha/packages/client"
	"github.com/syleron/pulseha/packages/config"
	"github.com/syleron/pulseha/rpc"
)

// setupCLI initializes the CLI commands
func setupCLI() *cobra.Command {
	// Create the root command
	rootCmd := &cobra.Command{
		Use:   "pulseha",
		Short: "PulseHA - High Availability Daemon",
		Long:  `PulseHA is a high availability daemon that provides automatic failover capabilities for clusters.`,
	}

	// Add commands
	rootCmd.AddCommand(
		newQuorumCmd(),
		// Add other commands here
	)

	return rootCmd
}

// newQuorumCmd creates the quorum command
func newQuorumCmd() *cobra.Command {
	quorumCmd := &cobra.Command{
		Use:   "quorum",
		Short: "Manage quorum voting",
		Long:  `Commands for managing quorum voting sessions and casting votes.`,
	}

	// Add subcommands
	quorumCmd.AddCommand(
		newQuorumEnableCmd(),
		newQuorumDisableCmd(),
		newQuorumConfigCmd(),
		listVotingSessionsCmd,
		getVotingSessionDetailsCmd,
		castVoteCmd,
	)

	return quorumCmd
}

// newQuorumEnableCmd creates the quorum enable command
func newQuorumEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable",
		Short: "Enable quorum voting",
		Long:  `Enable quorum voting for critical operations in the cluster.`,
		Run: func(cmd *cobra.Command, args []string) {
			// Load config
			cfg := config.New()
			if cfg == nil {
				fmt.Println("Error: Failed to load config")
				os.Exit(1)
			}

			// Create client
			c, err := client.New()
			if err != nil {
				fmt.Printf("Error: Failed to create client: %v\n", err)
				os.Exit(1)
			}

			// Get local node
			localNode, err := cfg.GetLocalNode()
			if err != nil {
				fmt.Printf("Error: Failed to get local node: %v\n", err)
				os.Exit(1)
			}

			// Connect to local node
			if err := c.Connect(localNode.IP, localNode.Port, false); err != nil {
				fmt.Printf("Error: Failed to connect to local node: %v\n", err)
				os.Exit(1)
			}
			defer c.Close()

			// Update config
			cfg.Pulse.QuorumEnabled = true
			if err := cfg.Save(); err != nil {
				fmt.Printf("Error: Failed to save config: %v\n", err)
				os.Exit(1)
			}

			// Sync config to other nodes
			configBytes, err := json.Marshal(cfg)
			if err != nil {
				fmt.Printf("Warning: Failed to serialize config: %v\n", err)
			} else {
				_, err = c.Send(client.SendConfigSync, &rpc.ConfigSyncRequest{
					Config: configBytes,
				})
				if err != nil {
					fmt.Printf("Warning: Failed to sync config to other nodes: %v\n", err)
				}
			}

			fmt.Println("Quorum voting has been enabled")
		},
	}
}

// newQuorumDisableCmd creates the quorum disable command
func newQuorumDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable quorum voting",
		Long:  `Disable quorum voting for critical operations in the cluster.`,
		Run: func(cmd *cobra.Command, args []string) {
			// Load config
			cfg := config.New()
			if cfg == nil {
				fmt.Println("Error: Failed to load config")
				os.Exit(1)
			}

			// Create client
			c, err := client.New()
			if err != nil {
				fmt.Printf("Error: Failed to create client: %v\n", err)
				os.Exit(1)
			}

			// Get local node
			localNode, err := cfg.GetLocalNode()
			if err != nil {
				fmt.Printf("Error: Failed to get local node: %v\n", err)
				os.Exit(1)
			}

			// Connect to local node
			if err := c.Connect(localNode.IP, localNode.Port, false); err != nil {
				fmt.Printf("Error: Failed to connect to local node: %v\n", err)
				os.Exit(1)
			}
			defer c.Close()

			// Update config
			cfg.Pulse.QuorumEnabled = false
			if err := cfg.Save(); err != nil {
				fmt.Printf("Error: Failed to save config: %v\n", err)
				os.Exit(1)
			}

			// Sync config to other nodes
			configBytes, err := json.Marshal(cfg)
			if err != nil {
				fmt.Printf("Warning: Failed to serialize config: %v\n", err)
			} else {
				_, err = c.Send(client.SendConfigSync, &rpc.ConfigSyncRequest{
					Config: configBytes,
				})
				if err != nil {
					fmt.Printf("Warning: Failed to sync config to other nodes: %v\n", err)
				}
			}

			fmt.Println("Quorum voting has been disabled")
		},
	}
}

// newQuorumConfigCmd creates the quorum config command
func newQuorumConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config [options]",
		Short: "Configure quorum settings",
		Long:  `Configure quorum settings such as minimum nodes and majority mode.`,
		Run: func(cmd *cobra.Command, args []string) {
			// Load config
			cfg := config.New()
			if cfg == nil {
				fmt.Println("Error: Failed to load config")
				os.Exit(1)
			}

			// Create client
			c, err := client.New()
			if err != nil {
				fmt.Printf("Error: Failed to create client: %v\n", err)
				os.Exit(1)
			}

			// Get local node
			localNode, err := cfg.GetLocalNode()
			if err != nil {
				fmt.Printf("Error: Failed to get local node: %v\n", err)
				os.Exit(1)
			}

			// Connect to local node
			if err := c.Connect(localNode.IP, localNode.Port, false); err != nil {
				fmt.Printf("Error: Failed to connect to local node: %v\n", err)
				os.Exit(1)
			}
			defer c.Close()

			// Get flag values
			minNodes, _ := cmd.Flags().GetInt("min-nodes")
			majorityMode, _ := cmd.Flags().GetBool("majority-mode")

			// Update config if flags are set
			configChanged := false

			if cmd.Flags().Changed("min-nodes") {
				cfg.Pulse.QuorumMinNodes = minNodes
				configChanged = true
			}

			if cmd.Flags().Changed("majority-mode") {
				cfg.Pulse.QuorumMajorityMode = majorityMode
				configChanged = true
			}

			// If no flags were set, display current config
			if !configChanged {
				fmt.Println("Current quorum configuration:")
				fmt.Printf("  Enabled: %v\n", cfg.Pulse.QuorumEnabled)
				fmt.Printf("  Minimum Nodes: %d\n", cfg.Pulse.QuorumMinNodes)
				fmt.Printf("  Majority Mode: %v\n", cfg.Pulse.QuorumMajorityMode)
				return
			}

			// Save config
			if err := cfg.Save(); err != nil {
				fmt.Printf("Error: Failed to save config: %v\n", err)
				os.Exit(1)
			}

			// Sync config to other nodes
			configBytes, err := json.Marshal(cfg)
			if err != nil {
				fmt.Printf("Warning: Failed to serialize config: %v\n", err)
			} else {
				_, err = c.Send(client.SendConfigSync, &rpc.ConfigSyncRequest{
					Config: configBytes,
				})
				if err != nil {
					fmt.Printf("Warning: Failed to sync config to other nodes: %v\n", err)
				}
			}

			fmt.Println("Quorum configuration updated successfully")
		},
	}

	// Add flags
	cmd.Flags().Int("min-nodes", 2, "Minimum number of nodes required for quorum")
	cmd.Flags().Bool("majority-mode", true, "Use majority of nodes for quorum instead of fixed minimum")

	return cmd
}

// Define quorum-related commands
var listVotingSessionsCmd = &cobra.Command{
	Use:   "list-sessions",
	Short: "List active voting sessions",
	Long:  `List all active voting sessions in the cluster.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Load config
		cfg := config.New()
		if cfg == nil {
			fmt.Println("Error: Failed to load config")
			os.Exit(1)
		}

		// Create client
		c, err := client.New()
		if err != nil {
			fmt.Printf("Error: Failed to create client: %v\n", err)
			os.Exit(1)
		}

		// Get local node
		localNode, err := cfg.GetLocalNode()
		if err != nil {
			fmt.Printf("Error: Failed to get local node: %v\n", err)
			os.Exit(1)
		}

		// Connect to local node
		if err := c.Connect(localNode.IP, localNode.Port, false); err != nil {
			fmt.Printf("Error: Failed to connect to local node: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		includeCompleted, _ := cmd.Flags().GetBool("include-completed")
		resp, err := c.GetVotingSessions(includeCompleted)
		if err != nil {
			fmt.Printf("Error: Failed to get voting sessions: %v\n", err)
			os.Exit(1)
		}

		if !resp.Success {
			fmt.Printf("Error: %s\n", resp.Message)
			os.Exit(1)
		}

		if len(resp.Sessions) == 0 {
			fmt.Println("No voting sessions found.")
			return
		}

		// Print sessions in a table format
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tTYPE\tSUBJECT\tSTATUS\tYES\tNO\tTOTAL\tQUORUM MET\tSTARTED\tENDS")
		for _, session := range resp.Sessions {
			status := "Active"
			yes := "-"
			no := "-"
			total := "-"
			quorumMet := "-"
			if session.Completed {
				status = "Completed"
				if session.Result != nil {
					yes = fmt.Sprintf("%d", session.Result.YesCount)
					no = fmt.Sprintf("%d", session.Result.NoCount)
					total = fmt.Sprintf("%d", session.Result.TotalVotes)
					if session.Result.QuorumMet {
						quorumMet = "Yes"
					} else {
						quorumMet = "No"
					}
				}
			}

			startTime := time.Unix(session.StartTime, 0).Format(time.RFC3339)
			endTime := time.Unix(session.EndTime, 0).Format(time.RFC3339)

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				session.Id,
				session.Type.String(),
				session.Subject,
				status,
				yes,
				no,
				total,
				quorumMet,
				startTime,
				endTime,
			)
		}
		w.Flush()
	},
}

var getVotingSessionDetailsCmd = &cobra.Command{
	Use:   "session-details [session-id]",
	Short: "Get details of a voting session",
	Long:  `Get detailed information about a specific voting session, including all votes cast.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		sessionID := args[0]

		// Load config
		cfg := config.New()
		if cfg == nil {
			fmt.Println("Error: Failed to load config")
			os.Exit(1)
		}

		// Create client
		c, err := client.New()
		if err != nil {
			fmt.Printf("Error: Failed to create client: %v\n", err)
			os.Exit(1)
		}

		// Get local node
		localNode, err := cfg.GetLocalNode()
		if err != nil {
			fmt.Printf("Error: Failed to get local node: %v\n", err)
			os.Exit(1)
		}

		// Connect to local node
		if err := c.Connect(localNode.IP, localNode.Port, false); err != nil {
			fmt.Printf("Error: Failed to connect to local node: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		resp, err := c.GetVotingSessionDetails(sessionID)
		if err != nil {
			fmt.Printf("Error: Failed to get voting session details: %v\n", err)
			os.Exit(1)
		}

		if !resp.Success {
			fmt.Printf("Error: %s\n", resp.Message)
			os.Exit(1)
		}

		session := resp.Session
		fmt.Println("Session Details:")
		fmt.Printf("  ID:          %s\n", session.Id)
		fmt.Printf("  Type:        %s\n", session.Type.String())
		fmt.Printf("  Subject:     %s\n", session.Subject)
		fmt.Printf("  Description: %s\n", session.Description)
		fmt.Printf("  Started:     %s\n", time.Unix(session.StartTime, 0).Format(time.RFC3339))
		fmt.Printf("  Ends:        %s\n", time.Unix(session.EndTime, 0).Format(time.RFC3339))

		status := "Active"
		if session.Completed {
			status = "Completed"
		}
		fmt.Printf("  Status:      %s\n", status)

		if session.Result != nil {
			fmt.Println("\nResult:")
			fmt.Printf("  Passed:      %v\n", session.Result.Passed)
			fmt.Printf("  Quorum Met:  %v\n", session.Result.QuorumMet)
			fmt.Printf("  Yes Votes:   %d\n", session.Result.YesCount)
			fmt.Printf("  No Votes:    %d\n", session.Result.NoCount)
			fmt.Printf("  Total Votes: %d\n", session.Result.TotalVotes)
			fmt.Printf("  Completed:   %s\n", time.Unix(session.Result.CompletedAt, 0).Format(time.RFC3339))
		}

		if len(resp.Votes) > 0 {
			fmt.Println("\nVotes:")
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "VOTER ID\tDECISION\tTIMESTAMP")
			for _, vote := range resp.Votes {
				fmt.Fprintf(w, "%s\t%s\t%s\n",
					vote.VoterId,
					vote.Decision.String(),
					time.Unix(vote.Timestamp, 0).Format(time.RFC3339),
				)
			}
			w.Flush()
		} else {
			fmt.Println("\nNo votes cast yet.")
		}
	},
}

var castVoteCmd = &cobra.Command{
	Use:   "cast-vote [session-id] [decision]",
	Short: "Cast a vote in a voting session",
	Long:  `Cast a vote in a specific voting session. Decision must be 'yes', 'no', or 'abstain'.`,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		sessionID := args[0]
		decisionStr := strings.ToLower(args[1])

		var decision rpc.VoteDecision
		switch decisionStr {
		case "yes":
			decision = rpc.VoteDecision_YES
		case "no":
			decision = rpc.VoteDecision_NO
		case "abstain":
			decision = rpc.VoteDecision_ABSTAIN
		default:
			fmt.Println("Error: Invalid decision. Must be 'yes', 'no', or 'abstain'.")
			os.Exit(1)
		}

		// Load config
		cfg := config.New()
		if cfg == nil {
			fmt.Println("Error: Failed to load config")
			os.Exit(1)
		}

		// Create client
		c, err := client.New()
		if err != nil {
			fmt.Printf("Error: Failed to create client: %v\n", err)
			os.Exit(1)
		}

		// Get local node
		localNode, err := cfg.GetLocalNode()
		if err != nil {
			fmt.Printf("Error: Failed to get local node: %v\n", err)
			os.Exit(1)
		}

		// Connect to local node
		if err := c.Connect(localNode.IP, localNode.Port, false); err != nil {
			fmt.Printf("Error: Failed to connect to local node: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		// Get our node ID
		nodeID, err := cfg.GetLocalNodeUUID()
		if err != nil {
			fmt.Printf("Error: Failed to get local node ID: %v\n", err)
			os.Exit(1)
		}

		resp, err := c.CastVote(sessionID, nodeID, decision)
		if err != nil {
			fmt.Printf("Error: Failed to cast vote: %v\n", err)
			os.Exit(1)
		}

		if !resp.Success {
			fmt.Printf("Error: %s\n", resp.Message)
			os.Exit(1)
		}

		fmt.Println("Vote cast successfully.")
	},
}

// Initialize flags for the quorum commands
func init() {
	// Add flags for the list-sessions command
	listVotingSessionsCmd.Flags().Bool("include-completed", false, "Include completed voting sessions")
}
