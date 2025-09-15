package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/charmbracelet/log"
	"github.com/google/uuid"
	"github.com/syleron/pulseha/internal/client"
	"github.com/syleron/pulseha/internal/membership"
	"github.com/syleron/pulseha/internal/quorum"
	"github.com/syleron/pulseha/packages/config"
	"github.com/syleron/pulseha/packages/network"
	"github.com/syleron/pulseha/packages/security"
	"github.com/syleron/pulseha/packages/utils"
	rpc "github.com/syleron/pulseha/rpc"
	"google.golang.org/grpc"
)

// Server represents the PulseHA server
type Server struct {
	sync.RWMutex
	config        *config.Config
	logger        *log.Logger
	memberList    *membership.MemberList
	healthCheck   *membership.HealthChecker
	ipMonitor     *membership.IPMonitor
	quorumManager *quorum.QuorumManager
	quorumHandler *quorum.RPCHandler
	grpcServer    *grpc.Server
	cliServer     *grpc.Server
	rpc.UnimplementedCLIServer
	rpc.UnimplementedServerServer
	// Convergence state
	clusterEpoch int64
	leaderID     string
}

// NewServer creates a new PulseHA server instance
func NewServer(cfg *config.Config, logger *log.Logger, memberList *membership.MemberList, healthCheck *membership.HealthChecker) *Server {
	// Create the quorum manager
	quorumMgr := quorum.NewQuorumManager(cfg, logger)

	// Create the quorum RPC handler
	quorumHandler := quorum.NewRPCHandler(quorumMgr, logger)

	// Create IP monitor
	ipMonitor := membership.NewIPMonitor(memberList, logger)

	// Set IP monitor reference in member list
	memberList.SetIPMonitor(ipMonitor)

	// Create server
	s := &Server{
		config:        cfg,
		logger:        logger,
		memberList:    memberList,
		healthCheck:   healthCheck,
		ipMonitor:     ipMonitor,
		quorumManager: quorumMgr,
		quorumHandler: quorumHandler,
		clusterEpoch:  0,
		leaderID:      "",
	}

	// Set server reference in health checker
	healthCheck.SetServerReference(s)

	return s
}

// Start initializes and starts the server components
func (s *Server) Start() error {
	s.Lock()
	defer s.Unlock()

	// Verify config is loaded
	s.logger.Debug("Verifying server configuration...")
	if s.config == nil {
		return fmt.Errorf("server config is nil")
	}

	// Load initial members from config
	s.logger.Debug("Loading initial members from configuration...")
	if s.memberList == nil {
		return fmt.Errorf("member list is nil")
	}
	if err := s.loadInitialMembers(); err != nil {
		return fmt.Errorf("failed to load initial members: %v", err)
	}

	// Start CLI server on localhost (ephemeral in tests to avoid conflicts)
	cliAddr := "127.0.0.1:8080"
	if os.Getenv("PULSEHA_TEST") == "true" {
		cliAddr = "127.0.0.1:0"
	}
	s.logger.Debug("Starting CLI gRPC server", "addr", cliAddr)
	s.cliServer = grpc.NewServer()
	// Register both CLI and Server services on the local listener so local operations (e.g., ConfigSync) work pre-cluster
	rpc.RegisterServerServer(s.cliServer, s)
	rpc.RegisterCLIServer(s.cliServer, s)
	cliListener, err := net.Listen("tcp", cliAddr)
	if err != nil {
		return fmt.Errorf("failed to listen for CLI server on %s: %v", cliAddr, err)
	}
	go func() {
		s.logger.Debug("Serving CLI gRPC", "addr", cliListener.Addr().String())
		if err := s.cliServer.Serve(cliListener); err != nil {
			s.logger.Error("CLI server failed", "error", err)
		}
	}()

	// Attempt to start cluster server ONLY if configuration is present
	var localNode config.Node
	localNode, err = s.config.GetLocalNodeForBinding()
	if err == nil {
		if err := s.startClusterListener(localNode); err != nil {
			return err
		}
	} else {
		s.logger.Info("No cluster binding configuration found; cluster RPC server not started. CLI is available on 127.0.0.1:8080 for bootstrap.")
	}

	// Generate certificates if they don't exist
	s.logger.Debug("Checking/Generating TLS certificates...")
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("failed to get hostname: %v", err)
	}
	if os.Getenv("PULSEHA_TEST") != "true" {
		if err := security.GenerateCertificates(hostname); err != nil {
			s.logger.Warn("Failed to generate certificates, continuing without TLS", "error", err)
		}
	} else {
		s.logger.Debug("PULSEHA_TEST=true: skipping certificate generation")
	}

	// Set server reference in health checker
	s.logger.Debug("Setting server reference in health checker...")
	if s.healthCheck != nil {
		s.healthCheck.SetServerReference(s)
		s.logger.Debug("Server reference set in health checker")
	} else {
		s.logger.Warn("Health checker is nil, cannot set server reference")
	}

	// Initialize quorum manager with node count
	s.logger.Debug("Initializing quorum manager...")
	if s.quorumManager != nil {
		nodeCount := s.memberList.GetMemberCount()
		s.quorumManager.UpdateNodeCount(nodeCount)
		s.quorumManager.Start()
		s.logger.Debug("Quorum manager started with node count: ", nodeCount)
	} else {
		s.logger.Warn("Quorum manager is nil, quorum voting will not be available")
	}

	// Register quorum RPC handlers if available
	if s.quorumHandler != nil {
		s.logger.Debug("Registering quorum RPC handlers...")
	} else {
		s.logger.Warn("Quorum handler is nil, quorum RPC methods will not be available")
	}

	// Start the health checker
	s.startHealthChecker()

	// Start the IP monitor
	if err := s.ipMonitor.Start(); err != nil {
		s.logger.Error("Failed to start IP monitor", "error", err)
		// Continue anyway, as this is not critical
	}

	// Only start health checker if we have a configured cluster
	if s.config.ClusterCheck() {
		s.startHealthChecker()
	} else {
		s.logger.Debug("No cluster configured; waiting for explicit resync after join")
	}

	return nil
}

// startClusterListener starts the gRPC server that handles inter-node RPC on the configured bind address
func (s *Server) startClusterListener(localNode config.Node) error {
	s.logger.Debugf("Starting cluster RPC server on %s:%s...", localNode.IP, localNode.Port)

	// Create gRPC server if needed
	if s.grpcServer == nil {
		s.grpcServer = grpc.NewServer()
		rpc.RegisterServerServer(s.grpcServer, s)
		// Also register CLI RPCs on the cluster listener to support remote operations like Join
		rpc.RegisterCLIServer(s.grpcServer, s)
	}

	address := fmt.Sprintf("%s:%s", localNode.IP, localNode.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", address, err)
	}

	// If bound to an ephemeral port, record the actual port in config
	if localNode.Port == "0" {
		if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok {
			actualPort := strconv.Itoa(tcpAddr.Port)
			if localID, e := s.config.GetLocalNodeUUID(); e == nil {
				if n := s.config.Nodes[localID]; n != nil {
					n.Port = actualPort
					_ = s.config.Save()
					s.logger.Debugf("Updated local node port to actual bound port: %s", actualPort)
				}
			}
		}
	}

	go func() {
		s.logger.Debug("Serving cluster gRPC", "addr", listener.Addr().String())
		if err := s.grpcServer.Serve(listener); err != nil {
			s.logger.Error("Cluster gRPC server failed", "error", err)
		}
	}()

	return nil
}

// startHealthChecker starts the health checker with the configured interval
func (s *Server) startHealthChecker() {
	s.logger.Debug("Starting health checker...")
	if s.healthCheck == nil {
		s.logger.Error("Health checker is nil, cannot start")
		return
	}

	// Check if health checker is already running
	if s.healthCheck.IsRunning() {
		s.logger.Debug("Health checker is already running")
		return
	}

	// Get interval from config or use default
	interval := 5 * time.Second
	if s.config.Pulse.HealthCheckInterval > 0 {
		interval = time.Duration(s.config.Pulse.HealthCheckInterval) * time.Millisecond
		s.logger.Infof("Using configured health check interval: %v", interval)
	} else {
		s.logger.Infof("Using default health check interval: %v", interval)
	}

	s.logger.Info("Initializing health checker", "interval", interval)
	s.healthCheck.Start(interval)
	s.logger.Info("Health checker started successfully")
}

// Stop gracefully shuts down the server
func (s *Server) Stop() {
	s.Lock()
	defer s.Unlock()

	s.logger.Info("Stopping PulseHA server")

	// Best-effort: drop all configured VIPs on local node before stopping
	if s.config != nil {
		if localID, err := s.config.GetLocalNodeUUID(); err == nil {
			if node := s.config.Nodes[localID]; node != nil {
				for iface, groups := range node.IPGroups {
					for _, g := range groups {
						if ips, ok := s.config.Groups[g]; ok {
							if len(ips) > 0 {
								_, _ = s.BringDownIP(context.Background(), &rpc.DownIpRequest{Iface: iface, Ips: ips})
							}
						}
					}
				}
			}
		}
	}

	// Stop the health checker
	if s.healthCheck != nil {
		s.healthCheck.Stop()
	}

	// Stop the IP monitor
	if s.ipMonitor != nil {
		s.ipMonitor.Stop()
	}

	// Stop the gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// Stop the CLI gRPC server
	if s.cliServer != nil {
		s.cliServer.GracefulStop()
	}

	s.logger.Info("PulseHA server stopped")
}

// loadInitialMembers loads members from config into the member list
func (s *Server) loadInitialMembers() error {
	s.logger.Info("Beginning initial member loading process...")

	if s.config == nil {
		s.logger.Error("FATAL: Cannot load members - config is nil!")
		return fmt.Errorf("config is nil")
	}
	s.logger.Debug("Config validation passed")

	if s.config.Nodes == nil {
		s.logger.Info("No nodes section found in config, starting with empty member list")
		return nil
	}
	s.logger.Debug("Nodes section found in config")

	nodeCount := len(s.config.Nodes)
	s.logger.Infof("Found %d node(s) in configuration", nodeCount)

	// Log the actual nodes found
	s.logger.Info("Nodes in configuration:")
	for id, node := range s.config.Nodes {
		s.logger.Infof("  - %s (IP: %s, Port: %s)", id, node.IP, node.Port)
	}

	for id, node := range s.config.Nodes {
		s.logger.Infof("Processing node: %s", id)

		// Check if member already exists
		if existingMember := s.memberList.GetMemberByID(id); existingMember != nil {
			s.logger.Debugf("Member %s already exists in member list, skipping", id)
			continue
		}

		if err := s.memberList.AddMember(id, node.Hostname, node.IP, node.Port); err != nil {
			s.logger.Error("FATAL: Failed to add member", "id", id, "error", err)
			return fmt.Errorf("failed to add member %s: %v", id, err)
		}
		s.logger.Infof("Successfully added member: %s", id)

		// Get the member we just added
		if member := s.memberList.GetMemberByID(id); member != nil {
			s.logger.Debugf("Verified member %s exists in member list", id)

			// Set member details from config
			member.IP = node.IP
			member.Port = node.Port
			member.Hostname = node.Hostname

			// Determine initial status based on node role and cluster state
			localNodeID, err := s.config.GetLocalNodeUUID()
			isLocalNode := err == nil && id == localNodeID

			// Default to Unknown for all nodes initially - health checks will determine actual status
			member.Status = membership.StatusUnknown

			// For local node, we can set initial status based on cluster mode
			if isLocalNode {
				if s.config.Pulse.Mode == "active-passive" {
					// In active-passive mode, determine who should be active
					totalNodes := len(s.config.Nodes)

					if totalNodes == 1 {
						// Single node cluster - should be active
						member.Status = membership.StatusActive
						s.logger.Infof("Setting single node %s as Active", id)
					} else {
						// Multi-node cluster - start as passive, election will determine active
						member.Status = membership.StatusPassive
						s.logger.Infof("Setting local node %s as Passive (election will determine active)", id)
					}
				} else {
					// Active-active mode - start as passive
					member.Status = membership.StatusPassive
					s.logger.Infof("Setting local node %s as Passive in active-active mode", id)
				}
			} else {
				// Remote nodes start as Unknown until health checks establish connection
				member.Status = membership.StatusUnknown
				s.logger.Infof("Setting remote node %s as Unknown (will be determined via health checks)", id)
			}

			// No longer try to determine status based on node order
			// Let the health checks and election process handle this

			s.logger.Debugf("Set initial details for member %s: IP=%s, Port=%s, Hostname=%s, Status=%s",
				id, member.IP, member.Port, member.Hostname, membership.StatusToString(member.Status))
		} else {
			s.logger.Warnf("Member %s was not found in member list after adding!", id)
		}
	}

	s.logger.Info("All members loaded successfully from configuration")
	s.logger.Debugf("Final member list contains %d members", len(s.memberList.Members))

	// After members are loaded, perform one-shot VIP reconcile on local node
	go func() {
		// small delay to ensure listeners up
		time.Sleep(500 * time.Millisecond)
		if localID, err := s.config.GetLocalNodeUUID(); err == nil {
			member := s.memberList.GetMemberByID(localID)
			node := s.config.Nodes[localID]
			if member != nil && node != nil {
				if member.Status == membership.StatusActive {
					// Bring up any missing expected VIPs
					for iface, groups := range node.IPGroups {
						var ips []string
						for _, g := range groups {
							if gips, ok := s.config.Groups[g]; ok {
								ips = append(ips, gips...)
							}
						}
						if len(ips) > 0 {
							_, _ = s.BringUpIP(context.Background(), &rpc.UpIpRequest{Iface: iface, Ips: ips})
						}
					}
				} else {
					// Passive: drop any VIPs found on local interfaces
					for iface, groups := range node.IPGroups {
						var ips []string
						for _, g := range groups {
							if gips, ok := s.config.Groups[g]; ok {
								ips = append(ips, gips...)
							}
						}
						if len(ips) > 0 {
							_, _ = s.BringDownIP(context.Background(), &rpc.DownIpRequest{Iface: iface, Ips: ips})
						}
					}
				}
			}
		}
	}()

	return nil
}

// HandleNodeJoin processes a new node joining the cluster
func (s *Server) HandleNodeJoin(ctx context.Context, req *rpc.JoinRequest) (*rpc.JoinResponse, error) {
	s.logger.Infof("Handling join request from node: %s", req.Hostname)
	s.logger.Debugf("Join request details - NodeID: %s, BindIP: %s, BindPort: %s, Token provided: %v",
		req.NodeId, req.BindIp, req.BindPort, req.Token != "")

	// Check if this is initial cluster creation
	if len(s.memberList.Members) == 0 && req.Token == "" {
		s.logger.Info("Initializing new cluster with first node: ", req.Hostname)

		// Node ID must be provided
		if req.NodeId == "" {
			return &rpc.JoinResponse{
				Success: false,
				Message: "node_id is required",
			}, nil
		}
		nodeID := req.NodeId
		s.logger.Debugf("Using node_id: %s", nodeID)

		// Add the node to the member list
		if err := s.memberList.AddMember(nodeID, req.Hostname, req.BindIp, req.BindPort); err != nil {
			s.logger.Error("Failed to add member to member list", "error", err)
			return &rpc.JoinResponse{
				Success: false,
				Message: fmt.Sprintf("failed to add member: %v", err),
			}, nil
		}

		// Set the cluster token
		s.config.Pulse.ClusterToken = uuid.New().String()
		s.logger.Debugf("Generated cluster token: %s", s.config.Pulse.ClusterToken)

		// Save the config
		if err := s.config.Save(); err != nil {
			s.logger.Error("Failed to save config", "error", err)
			return &rpc.JoinResponse{
				Success: false,
				Message: fmt.Sprintf("failed to save config: %v", err),
			}, nil
		}

		return &rpc.JoinResponse{
			Success: true,
			NodeId:  nodeID,
			Message: "Successfully initialized new cluster",
		}, nil
	}

	// Validate cluster token for existing cluster
	if os.Getenv("PULSEHA_TEST") != "true" {
		s.logger.Debugf("Validating cluster token for join...")
		clusterToken := s.config.Pulse.ClusterToken // Direct read - config token shouldn't change during join
		s.logger.Debugf("Expected token: %s, Received token: %s", clusterToken, req.Token)

		if req.Token != clusterToken {
			s.logger.Warn("Invalid cluster join token received")
			return &rpc.JoinResponse{
				Success: false,
				Message: "Invalid cluster token",
			}, nil
		}
		s.logger.Debugf("Token validation passed")
	} else {
		s.logger.Debug("PULSEHA_TEST=true: skipping token validation for join")
	}

	// Node ID must be provided
	if req.NodeId == "" {
		return &rpc.JoinResponse{
			Success: false,
			Message: "node_id is required",
		}, nil
	}
	nodeID := req.NodeId
	s.logger.Debugf("Using node_id: %s", nodeID)

	// Add the node to the member list
	s.logger.Debugf("Adding member %s to member list...", nodeID)
	if err := s.memberList.AddMember(nodeID, req.Hostname, req.BindIp, req.BindPort); err != nil {
		s.logger.Error("Failed to add member to member list", "error", err)
		return &rpc.JoinResponse{
			Success: false,
			Message: fmt.Sprintf("failed to add member: %v", err),
		}, nil
	}
	s.logger.Debugf("Member %s added to member list successfully", nodeID)

	// Add node to config (need to lock config access)
	s.logger.Debugf("About to update config...")
	// Use config's lock instead of server's lock to avoid deadlock
	s.config.Lock()
	s.logger.Debugf("Config lock acquired, updating nodes...")
	if s.config.Nodes == nil {
		s.config.Nodes = make(map[string]*config.Node)
	}
	s.config.Nodes[nodeID] = &config.Node{
		Hostname: req.Hostname,
		IP:       req.BindIp,
		Port:     req.BindPort,
		IPGroups: make(map[string][]string),
	}
	s.logger.Debugf("Config updated, releasing config lock...")
	s.config.Unlock()
	s.logger.Debugf("Config lock released")

	s.logger.Infof("Successfully joined node %s (ID: %s) to cluster", req.Hostname, nodeID)

	// Save the config synchronously to ensure it's available for health checks
	s.logger.Debugf("Saving config with new member %s...", nodeID)
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config after successful join", "error", err)
		// Still return success since member was added to memberList
	} else {
		s.logger.Debugf("Config saved successfully after node %s joined", req.Hostname)
		// Best-effort: broadcast updated configuration to all peers so they persist the new member
		configBytes, mErr := json.Marshal(s.config)
		if mErr == nil {
			localID, _ := s.config.GetLocalNodeUUID()
			for id, node := range s.config.Nodes {
				if id == localID {
					continue
				}
				remoteClient, cErr := client.New()
				if cErr != nil {
					continue
				}
				if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
					remoteClient.Close()
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, _ = remoteClient.Server().ConfigSync(ctx, &rpc.ConfigSyncRequest{Config: configBytes})
				cancel()
				remoteClient.Close()
			}
		}
	}

	// Marshal the complete cluster configuration to send to the joining node
	// Create an enhanced config that includes member status information
	type EnhancedConfig struct {
		*config.Config
		MemberStates map[string]membership.MemberStatus `json:"member_states"`
	}

	// Ensure there is an active node before broadcasting config; if none, set local as active
	{
		var hasActive bool
		for _, m := range s.memberList.Members {
			if m.Status == membership.StatusActive {
				hasActive = true
				break
			}
		}
		if !hasActive {
			if localID, err := s.config.GetLocalNodeUUID(); err == nil {
				if m := s.memberList.GetMemberByID(localID); m != nil {
					m.Status = membership.StatusActive
				}
			}
		}
	}

	// Collect current member states
	memberStates := make(map[string]membership.MemberStatus)
	for id, member := range s.memberList.Members {
		memberStates[id] = member.Status
		s.logger.Debugf("Adding member state for %s: %s", id, membership.StatusToString(member.Status))
	}

	enhancedConfig := EnhancedConfig{
		Config:       s.config,
		MemberStates: memberStates,
	}

	configBytes, err := json.Marshal(enhancedConfig)
	if err != nil {
		s.logger.Error("Failed to marshal config for joining node", "error", err)
		// Still return success but without config - node can sync later
		return &rpc.JoinResponse{
			Success: true,
			NodeId:  nodeID,
			Message: "Successfully joined cluster (config sync pending)",
		}, nil
	}

	s.logger.Debugf("Sending cluster configuration with member states to joining node %s", req.Hostname)
	return &rpc.JoinResponse{
		Success:       true,
		NodeId:        nodeID,
		Message:       "Successfully joined cluster",
		ClusterConfig: configBytes,
	}, nil
}

// HandleNodeLeave handles the node leave RPC call
func (s *Server) HandleNodeLeave(ctx context.Context, req *rpc.LeaveRequest) (*rpc.LeaveResponse, error) {
	s.Lock()
	defer s.Unlock()

	// Get node ID for the request
	var nodeID string

	if req.NodeId != "" {
		nodeID = req.NodeId
		s.logger.Debugf("Using provided node_id for leave: %s", nodeID)
	} else {
		// Neither node_id nor hostname provided
		return &rpc.LeaveResponse{
			Success: false,
			Message: "missing node_id",
		}, nil
	}

	// Get the member
	member := s.memberList.GetMemberByID(nodeID)
	if member == nil {
		return &rpc.LeaveResponse{
			Success: false,
			Message: fmt.Sprintf("node not found with ID %s", nodeID),
		}, nil
	}

	// We can't leave ourself from a cluster
	localNodeID, err := s.config.GetLocalNodeUUID()
	if err != nil {
		return &rpc.LeaveResponse{
			Success: false,
			Message: "Unable to get local node: " + err.Error(),
		}, nil
	}

	// If this is the local node, we need to stop the server
	if nodeID == localNodeID {
		s.logger.Info("Leaving cluster as local node")
		go func() {
			time.Sleep(1 * time.Second)
			s.Stop()
		}()
		return &rpc.LeaveResponse{
			Success: true,
			Message: "Successfully left the cluster",
		}, nil
	}

	// Remove the node from our member list
	if err := s.memberList.RemoveMember(nodeID); err != nil {
		s.logger.Error("Failed to remove member", "error", err)
		return &rpc.LeaveResponse{
			Success: false,
			Message: "Failed to remove member: " + err.Error(),
		}, nil
	}

	// Update our config to remove the node
	delete(s.config.Nodes, nodeID)

	// Success
	return &rpc.LeaveResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully removed node %s from the cluster", nodeID),
	}, nil
}

// GetClusterStatus returns the current status of all nodes
func (s *Server) GetClusterStatus(ctx context.Context, req *rpc.StatusRequest) (*rpc.StatusResponse, error) {
	s.RLock()
	defer s.RUnlock()

	var members []*rpc.Member
	for _, member := range s.memberList.Members {
		health := member.GetHealthStatus()
		var st rpc.MemberStatusEnum
		switch health.Status {
		case membership.StatusActive:
			st = rpc.MemberStatusEnum_MEMBER_STATUS_ACTIVE
		case membership.StatusPassive:
			st = rpc.MemberStatusEnum_MEMBER_STATUS_PASSIVE
		case membership.StatusPartialActive:
			st = rpc.MemberStatusEnum_MEMBER_STATUS_PARTIAL_ACTIVE
		default:
			st = rpc.MemberStatusEnum_MEMBER_STATUS_UNKNOWN
		}

		lastResp := ""
		if !health.LastResponse.IsZero() {
			lastResp = health.LastResponse.Format(time.RFC3339)
		}

		members = append(members, &rpc.Member{
			Hostname:      health.Hostname,
			Status:        st,
			ActiveIps:     health.ActiveIPs,
			LastResponse:  lastResp,
			Latency:       health.Latency,
			PartialActive: health.PartialActive,
			Ip:            member.IP,
			Port:          member.Port,
			NodeId:        member.ID,
		})
	}

	// Build groups information from server config so clients don't need local config
	var groups []*rpc.GroupInfo
	for groupName, ips := range s.config.Groups {
		group := &rpc.GroupInfo{
			Name: groupName,
			Ips:  ips,
		}

		// Find assignments for this group
		for id, node := range s.config.Nodes {
			for iface, assignedGroups := range node.IPGroups {
				for _, g := range assignedGroups {
					if g == groupName {
						group.Assignments = append(group.Assignments, &rpc.GroupAssignment{
							Interface: iface,
							NodeId:    id,
						})
					}
				}
			}
		}

		groups = append(groups, group)
	}

	return &rpc.StatusResponse{
		Members: members,
		Groups:  groups,
	}, nil
}

// Status implements the CLI.Status RPC method
func (s *Server) Status(ctx context.Context, req *rpc.StatusRequest) (*rpc.StatusResponse, error) {
	return s.GetClusterStatus(ctx, req)
}

// PromoteNode promotes a node to active status
func (s *Server) PromoteNode(ctx context.Context, req *rpc.PromoteRequest) (*rpc.PromoteResponse, error) {
	s.Lock()
	defer s.Unlock()

	s.logger.Infof("Handling promote request for node ID: %s", req.NodeId)

	// Get node ID for the request
	var nodeID string

	if req.NodeId != "" {
		nodeID = req.NodeId
		s.logger.Debugf("Using provided node_id for promote: %s", nodeID)
	} else {
		// No node_id provided
		return &rpc.PromoteResponse{
			Success: false,
			Message: "missing node_id",
		}, nil
	}

	// Get the member
	member := s.memberList.GetMemberByID(nodeID)
	if member == nil {
		return &rpc.PromoteResponse{
			Success: false,
			Message: fmt.Sprintf("node not found with ID %s", nodeID),
		}, nil
	}

	// If the member is local, promote locally; otherwise, connect to the target node and ask it to promote itself
	if member.IsLocal() {
		if err := member.MakeActive(req.Ips); err != nil {
			s.logger.Error("Failed to promote local member", "error", err)
			return &rpc.PromoteResponse{Success: false, Message: "Failed to promote member: " + err.Error()}, nil
		}
	} else {
		node := s.config.Nodes[req.NodeId]
		if node == nil {
			return &rpc.PromoteResponse{Success: false, Message: "Node configuration not found"}, nil
		}
		remoteClient, err := client.New()
		if err != nil {
			return &rpc.PromoteResponse{Success: false, Message: "Failed to create client: " + err.Error()}, nil
		}
		defer remoteClient.Close()
		if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
			return &rpc.PromoteResponse{Success: false, Message: "Failed to connect to target node: " + err.Error()}, nil
		}
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rresp, rerr := remoteClient.CLI().Promote(ctx2, &rpc.PromoteRequest{NodeId: req.NodeId, Ips: req.Ips})
		if rerr != nil {
			return &rpc.PromoteResponse{Success: false, Message: "Remote promote failed: " + rerr.Error()}, nil
		}
		if !rresp.Success {
			return &rpc.PromoteResponse{Success: false, Message: rresp.Message}, nil
		}
		// Reflect the status change locally
		member.Status = membership.StatusActive
	}

	// Broadcast convergence state so peers adopt the same active (active-passive)
	states := make(map[string]membership.MemberStatus)
	for id, m := range s.memberList.Members {
		states[id] = m.Status
	}
	_ = s.BroadcastClusterState(states, s.GetClusterEpoch()+1, nodeID, nil)

	// Post-promotion: reconcile VIPs on local node
	go s.refreshLocalMonitorExpectedIPs()

	// Success
	return &rpc.PromoteResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully promoted node %s to active", req.NodeId),
	}, nil
}

// Join handles the CLI Join RPC call
func (s *Server) Join(ctx context.Context, req *rpc.JoinRequest) (*rpc.JoinResponse, error) {
	s.logger.Info("Received CLI Join request", "hostname", req.Hostname, "tokenProvided", req.Token != "")

	resp, err := s.HandleNodeJoin(ctx, req)
	if err != nil {
		s.logger.Error("CLI Join request failed", "error", err)
	} else {
		s.logger.Info("CLI Join request completed", "success", resp.Success, "message", resp.Message)
	}
	return resp, err
}

// Leave handles the CLI Leave RPC call
func (s *Server) Leave(ctx context.Context, req *rpc.LeaveRequest) (*rpc.LeaveResponse, error) {
	s.logger.Info("Received CLI Leave request", "node_id", req.NodeId)

	if !s.config.ClusterCheck() {
		return &rpc.LeaveResponse{Success: false, Message: "no cluster configured; nothing to leave"}, nil
	}

	// If no node_id provided, default to local node
	if req.NodeId == "" {
		if id, err := s.config.GetLocalNodeUUID(); err == nil {
			req.NodeId = id
		} else {
			return &rpc.LeaveResponse{Success: false, Message: "Unable to determine local node: " + err.Error()}, nil
		}
	}

	// Get the member
	member := s.memberList.GetMemberByID(req.NodeId)
	if member == nil {
		return &rpc.LeaveResponse{
			Success: false,
			Message: fmt.Sprintf("Node not found with ID: %s", req.NodeId),
		}, nil
	}

	// We can't leave ourself from a cluster
	localNodeID, err := s.config.GetLocalNodeUUID()
	if err != nil {
		return &rpc.LeaveResponse{
			Success: false,
			Message: "Unable to get local node: " + err.Error(),
		}, nil
	}

	// If this is the local node, we need to stop the server
	if member.ID == localNodeID {
		s.logger.Info("Leaving cluster as local node")

		// Broadcast removal of this node to all other peers before stopping
		for id, node := range s.config.Nodes {
			if id == localNodeID {
				continue
			}
			remoteClient, err := client.New()
			if err != nil {
				s.logger.Warn("Failed to create client for peer", "peer", id, "error", err)
				continue
			}
			if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
				remoteClient.Close()
				s.logger.Warn("Failed to connect to peer", "peer", id, "error", err)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = remoteClient.Server().Remove(ctx, &rpc.RemoveRequest{NodeId: localNodeID})
			cancel()
			remoteClient.Close()
			if err != nil {
				s.logger.Warn("Failed to propagate removal to peer", "peer", id, "error", err)
			}
		}

		// Stop the server asynchronously to allow RPC response to complete
		go func() {
			time.Sleep(1 * time.Second)
			s.Stop()
		}()
		return &rpc.LeaveResponse{
			Success: true,
			Message: "Successfully left the cluster",
		}, nil
	}

	// Remove the node from our member list
	if err := s.memberList.RemoveMember(member.ID); err != nil {
		s.logger.Error("Failed to remove member", "error", err)
		return &rpc.LeaveResponse{
			Success: false,
			Message: "Failed to remove member: " + err.Error(),
		}, nil
	}

	// Update our config to remove the node
	delete(s.config.Nodes, member.ID)

	// Success
	return &rpc.LeaveResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully removed node %s from the cluster", member.ID),
	}, nil
}

// Promote handles the CLI Promote RPC call
func (s *Server) Promote(ctx context.Context, req *rpc.PromoteRequest) (*rpc.PromoteResponse, error) {
	s.logger.Infof("Received promote request for node ID: %s", req.NodeId)

	if !s.config.ClusterCheck() {
		return &rpc.PromoteResponse{Success: false, Message: "no cluster configured"}, nil
	}

	if req.NodeId == "" {
		return &rpc.PromoteResponse{
			Success: false,
			Message: "node_id is required",
		}, nil
	}

	// If another node is currently Active in active-passive mode, demote it first to avoid conflicts.
	// Capture the previous active ID for deterministic VIP transfer later.
	prevActiveID := ""
	if s.config.Pulse.Mode == "active-passive" {
		for id, m := range s.memberList.Members {
			if m.Status == membership.StatusActive {
				prevActiveID = id
				break
			}
		}
		if prevActiveID != "" && prevActiveID != req.NodeId {
			s.logger.Info("Demoting current active before promotion", "current_active", prevActiveID, "target", req.NodeId)
			// Best-effort demotion via RPC path (handles local/remote)
			if _, err := s.MakePassive(context.Background(), &rpc.MakePassiveRequest{NodeId: prevActiveID}); err != nil {
				s.logger.Warn("Failed to demote current active before promotion", "error", err)
			}
		}
	}

	// Get the member
	member := s.memberList.GetMemberByID(req.NodeId)
	if member == nil {
		return &rpc.PromoteResponse{
			Success: false,
			Message: fmt.Sprintf("Node not found with ID: %s", req.NodeId),
		}, nil
	}

	// If the member is local, promote locally; otherwise, connect to the target node and ask it to promote itself
	if member.IsLocal() {
		if err := member.MakeActive(req.Ips); err != nil {
			s.logger.Error("Failed to promote local member", "error", err)
			return &rpc.PromoteResponse{Success: false, Message: "Failed to promote member: " + err.Error()}, nil
		}
	} else {
		node := s.config.Nodes[req.NodeId]
		if node == nil {
			return &rpc.PromoteResponse{Success: false, Message: "Node configuration not found"}, nil
		}
		remoteClient, err := client.New()
		if err != nil {
			return &rpc.PromoteResponse{Success: false, Message: "Failed to create client: " + err.Error()}, nil
		}
		defer remoteClient.Close()
		if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
			return &rpc.PromoteResponse{Success: false, Message: "Failed to connect to target node: " + err.Error()}, nil
		}
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rresp, rerr := remoteClient.CLI().Promote(ctx2, &rpc.PromoteRequest{NodeId: req.NodeId, Ips: req.Ips})
		if rerr != nil {
			return &rpc.PromoteResponse{Success: false, Message: "Remote promote failed: " + rerr.Error()}, nil
		}
		if !rresp.Success {
			return &rpc.PromoteResponse{Success: false, Message: rresp.Message}, nil
		}
		// Reflect the status change locally
		member.Status = membership.StatusActive
	}

	// If active-passive, orchestrate floating IP transfer from old active to new active
	if s.config.Pulse.Mode == "active-passive" {
		// Collect all floating IPs defined in config
		var allIPs []string
		for _, ipList := range s.config.Groups {
			allIPs = append(allIPs, ipList...)
		}
		// Use previously captured active (before demotion)
		oldActiveID := prevActiveID
		if len(allIPs) > 0 {
			if err := s.OrchestrateIPFailover(oldActiveID, req.NodeId, allIPs); err != nil {
				s.logger.Warn("VIP transfer encountered issues", "error", err)
			}
		}
	}

	// Broadcast convergence state so peers adopt the same active (active-passive)
	states := make(map[string]membership.MemberStatus)
	for id, m := range s.memberList.Members {
		states[id] = m.Status
	}
	_ = s.BroadcastClusterState(states, s.GetClusterEpoch()+1, req.NodeId, nil)

	// Post-promotion: reconcile VIPs on local node
	go s.refreshLocalMonitorExpectedIPs()

	// Success
	return &rpc.PromoteResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully promoted node %s to active", req.NodeId),
	}, nil
}

// MakePassive handles the passive RPC call making one node passive
func (s *Server) MakePassive(ctx context.Context, req *rpc.MakePassiveRequest) (*rpc.MakePassiveResponse, error) {
	s.logger.Infof("Received make passive request for node ID: %s", req.NodeId)

	if req.NodeId == "" {
		return &rpc.MakePassiveResponse{
			Success: false,
			Message: "node_id is required",
		}, nil
	}

	// Get the member
	member := s.memberList.GetMemberByID(req.NodeId)
	if member == nil {
		return &rpc.MakePassiveResponse{
			Success: false,
			Message: fmt.Sprintf("Node not found with ID: %s", req.NodeId),
		}, nil
	}

	// If local, make passive locally; otherwise forward to remote node and reflect state
	if member.IsLocal() {
		// Proactively bring down all floating IPs assigned to this node per config
		var ipsToDrop []string
		if localNodeCfg := s.config.Nodes[member.ID]; localNodeCfg != nil {
			for _, groups := range localNodeCfg.IPGroups {
				for _, g := range groups {
					if ipList, ok := s.config.Groups[g]; ok {
						ipsToDrop = append(ipsToDrop, ipList...)
					}
				}
			}
		}
		if len(ipsToDrop) > 0 {
			if err := member.BringDownIPs(ipsToDrop); err != nil {
				s.logger.Warn("Failed to bring down IPs during demotion", "error", err)
			}
		}
		member.Status = membership.StatusPassive
		member.ActiveIPs = nil
		member.PartialActive = false
	} else {
		node := s.config.Nodes[req.NodeId]
		if node == nil {
			return &rpc.MakePassiveResponse{Success: false, Message: "Node configuration not found"}, nil
		}
		remoteClient, err := client.New()
		if err != nil {
			return &rpc.MakePassiveResponse{Success: false, Message: "Failed to create client: " + err.Error()}, nil
		}
		defer remoteClient.Close()
		if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
			return &rpc.MakePassiveResponse{Success: false, Message: "Failed to connect to target node: " + err.Error()}, nil
		}
		ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rresp, rerr := remoteClient.Server().MakePassive(ctx2, &rpc.MakePassiveRequest{NodeId: req.NodeId})
		if rerr != nil {
			return &rpc.MakePassiveResponse{Success: false, Message: "Remote make passive failed: " + rerr.Error()}, nil
		}
		if !rresp.Success {
			return &rpc.MakePassiveResponse{Success: false, Message: rresp.Message}, nil
		}
		// Reflect locally
		member.Status = membership.StatusPassive
		member.ActiveIPs = nil
		member.PartialActive = false
	}

	// Success
	// Update local monitor expectations based on new role
	s.refreshLocalMonitorExpectedIPs()
	return &rpc.MakePassiveResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully made node %s passive", req.NodeId),
	}, nil
}

// HealthCheck handles the health check RPC call
func (s *Server) HealthCheck(ctx context.Context, req *rpc.HealthCheckRequest) (*rpc.HealthCheckResponse, error) {
	// Get the member
	var member *membership.Member

	if req.NodeId != "" {
		member = s.memberList.GetMemberByID(req.NodeId)
		if member == nil {
			return &rpc.HealthCheckResponse{
				Success: false,
				Message: fmt.Sprintf("Node not found with ID: %s", req.NodeId),
			}, nil
		}
		s.logger.Debugf("Found node by ID: %s (%s)", req.NodeId, member.Hostname)
	}

	if member == nil {
		return &rpc.HealthCheckResponse{
			Success: false,
			Message: "No node identifier provided",
		}, nil
	}

	// Update last response time
	member.LastHCResponse = time.Now()

	// Calculate and update latency
	latency := time.Since(member.LastHCResponse).String()
	member.Latency = latency
	s.logger.Debugf("Member %s latency: %s", member.Hostname, latency)

	// Return healthy response
	return &rpc.HealthCheckResponse{
		Success: true,
		Message: fmt.Sprintf("Node %s is healthy", member.Hostname),
	}, nil
}

// Remove removes a node from the cluster
func (s *Server) Remove(ctx context.Context, req *rpc.RemoveRequest) (*rpc.RemoveResponse, error) {
	s.logger.Infof("Received remove request for node ID: %s", req.NodeId)

	if req.NodeId == "" {
		return &rpc.RemoveResponse{
			Success: false,
			Message: "node_id is required",
		}, nil
	}

	// Get the member
	member := s.memberList.GetMemberByID(req.NodeId)
	if member == nil {
		return &rpc.RemoveResponse{
			Success: false,
			Message: fmt.Sprintf("Node not found with ID: %s", req.NodeId),
		}, nil
	}

	// Remove the node from our member list
	if err := s.memberList.RemoveMember(member.ID); err != nil {
		s.logger.Error("Failed to remove member", "error", err)
		return &rpc.RemoveResponse{
			Success: false,
			Message: "Failed to remove member: " + err.Error(),
		}, nil
	}

	// Update our config to remove the node
	delete(s.config.Nodes, member.ID)

	// Persist the updated configuration
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config after removing node", "error", err)
		return &rpc.RemoveResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	// Success
	return &rpc.RemoveResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully removed node %s from the cluster", req.NodeId),
	}, nil
}

// Reconfigure updates the server configuration in real-time
func (s *Server) Reconfigure() error {
	s.logger.Info("Reconfiguring PulseHA server...")

	s.Lock()
	defer s.Unlock()

	// Reload config
	s.logger.Debug("Reloading configuration...")
	if err := s.config.Reload(); err != nil {
		return fmt.Errorf("failed to reload config: %v", err)
	}

	// Get local node config
	s.logger.Debug("Getting updated local node configuration...")
	localNode, err := s.config.GetLocalNode()
	if err != nil {
		return fmt.Errorf("failed to get local node config: %v", err)
	}
	s.logger.Infof("Updated local node configuration: IP=%s, Port=%s", localNode.IP, localNode.Port)

	// Stop existing gRPC server if it exists
	if s.grpcServer != nil {
		s.logger.Debug("Stopping existing gRPC server...")
		s.grpcServer.GracefulStop()
	}

	// Create new gRPC server for cluster-only RPC and start listener
	s.logger.Debug("Creating new cluster gRPC server...")
	s.grpcServer = grpc.NewServer()
	rpc.RegisterServerServer(s.grpcServer, s)
	// Also register CLI service on the cluster listener for remote operations (e.g., join)
	rpc.RegisterCLIServer(s.grpcServer, s)

	s.logger.Debugf("Starting cluster listener on %s:%s...", localNode.IP, localNode.Port)
	if err := s.startClusterListener(localNode); err != nil {
		return fmt.Errorf("failed to start cluster listener: %v", err)
	}

	s.logger.Info("Server reconfiguration completed successfully")
	return nil
}

// GetMemberList returns the server's member list
func (s *Server) GetMemberList() *membership.MemberList {
	return s.memberList
}

// refreshLocalMonitorExpectedIPs updates the IP monitor's expected IPs for the local node
// Only enforces when the local member is Active; clears expectations when not active
func (s *Server) refreshLocalMonitorExpectedIPs() {
	if s.ipMonitor == nil {
		return
	}
	localID, err := s.config.GetLocalNodeUUID()
	if err != nil {
		return
	}
	member := s.memberList.GetMemberByID(localID)
	node := s.config.Nodes[localID]
	if member == nil || node == nil {
		return
	}

	if member.Status != membership.StatusActive {
		for iface := range node.IPGroups {
			s.ipMonitor.ClearExpectedIPs(iface)
		}
		// Actively drop configured floating IPs on passive role
		for iface, groups := range node.IPGroups {
			for _, g := range groups {
				if ips, ok := s.config.Groups[g]; ok {
					for _, ip := range ips {
						_, _ = s.BringDownIP(context.Background(), &rpc.DownIpRequest{Iface: iface, Ips: []string{ip}})
					}
				}
			}
		}
		return
	}

	for iface := range node.IPGroups {
		var ifaceIPs []string
		for _, g := range node.IPGroups[iface] {
			if ips, ok := s.config.Groups[g]; ok {
				ifaceIPs = append(ifaceIPs, ips...)
			}
		}
		s.ipMonitor.ClearExpectedIPs(iface)
		if len(ifaceIPs) > 0 {
			s.ipMonitor.UpdateExpectedIPs(iface, ifaceIPs)
			// Proactively bring up any missing expected IPs on this interface
			var missing []string
			for _, ip := range ifaceIPs {
				ipOnly, _ := utils.GetCIDR(ip)
				if ipOnly == nil {
					continue
				}
				exists, existingIface, _ := network.CheckIfIPExists(ipOnly.String())
				if !exists || existingIface != iface {
					missing = append(missing, ip)
				}
			}
			if len(missing) > 0 {
				_, _ = s.BringUpIP(context.Background(), &rpc.UpIpRequest{Iface: iface, Ips: missing})
			}
		}
	}
}

// SetMode handles changing the cluster operation mode
func (s *Server) SetMode(ctx context.Context, req *rpc.SetModeRequest) (*rpc.SetModeResponse, error) {
	s.logger.Infof("Received request to change cluster mode to: %s", req.Mode)
	s.Lock()
	defer s.Unlock()

	// Validate request
	if req.Mode != "active-passive" && req.Mode != "active-active" {
		return &rpc.SetModeResponse{
			Success: false,
			Message: fmt.Sprintf("invalid mode: %s", req.Mode),
		}, nil
	}

	// Get current mode
	currentMode := "active-passive" // Default mode
	if s.config.Pulse.Mode == "active-active" {
		currentMode = "active-active"
	}

	// If mode is not changing, return early
	if currentMode == req.Mode {
		return &rpc.SetModeResponse{
			Success: true,
			Message: fmt.Sprintf("cluster is already in %s mode", req.Mode),
		}, nil
	}

	// Update mode in config
	s.config.Pulse.Mode = req.Mode

	// Save config
	if err := s.config.Save(); err != nil {
		return &rpc.SetModeResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	// If switching to active-active, redistribute IPs
	if req.Mode == "active-active" {
		s.logger.Info("Redistributing IPs for active-active mode")
		var allIPs []string
		for _, member := range s.memberList.Members {
			allIPs = append(allIPs, member.ActiveIPs...)
			member.ActiveIPs = nil // Clear current assignments
		}

		if err := s.memberList.RedistributeIPs(allIPs); err != nil {
			s.logger.Error("Failed to redistribute IPs", "error", err)
			// Continue anyway as the mode change is already saved
		}
	}

	// If switching to active-passive, move all IPs to the active node
	if req.Mode == "active-passive" {
		s.logger.Info("Moving all IPs to active node")
		var activeNode *membership.Member
		var allIPs []string

		// Find active node and collect all IPs
		for _, member := range s.memberList.Members {
			if member.Status == membership.StatusActive {
				activeNode = member
			}
			allIPs = append(allIPs, member.ActiveIPs...)
			member.ActiveIPs = nil // Clear current assignments
		}

		// Assign all IPs to active node
		if activeNode != nil {
			activeNode.ActiveIPs = allIPs
		}
	}

	s.logger.Infof("Successfully changed cluster mode to: %s", req.Mode)
	// Update local monitor expectations based on new role
	s.refreshLocalMonitorExpectedIPs()
	return &rpc.SetModeResponse{
		Success: true,
		Message: fmt.Sprintf("cluster mode changed to %s", req.Mode),
	}, nil
}

// CreateGroup implements the CLI.CreateGroup RPC method
func (s *Server) CreateGroup(ctx context.Context, req *rpc.CreateGroupRequest) (*rpc.CreateGroupResponse, error) {
	s.logger.Infof("Received CreateGroup request for group: %s", req.Name)
	s.Lock()
	defer s.Unlock()

	if !s.config.ClusterCheck() {
		return &rpc.CreateGroupResponse{Success: false, Message: "no cluster configured"}, nil
	}

	// Check if group already exists
	if _, exists := s.config.Groups[req.Name]; exists {
		s.logger.Infof("Group %s already exists; treating as success", req.Name)
		return &rpc.CreateGroupResponse{
			Success: true,
			Message: fmt.Sprintf("group %s already exists", req.Name),
		}, nil
	}

	// Initialize Groups map if it doesn't exist
	if s.config.Groups == nil {
		s.config.Groups = make(map[string][]string)
	}

	// Create new empty group
	s.config.Groups[req.Name] = make([]string, 0)

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.CreateGroupResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	s.logger.Infof("Successfully created group: %s", req.Name)
	return &rpc.CreateGroupResponse{
		Success: true,
		Message: fmt.Sprintf("group %s created successfully", req.Name),
	}, nil
}

// AddIPToGroup implements the CLI.AddIPToGroup RPC method
func (s *Server) AddIPToGroup(ctx context.Context, req *rpc.AddIPToGroupRequest) (*rpc.AddIPToGroupResponse, error) {
	s.logger.Infof("Received AddIPToGroup request for group: %s, IP: %s", req.GroupName, req.Ip)
	s.Lock()
	defer s.Unlock()

	if !s.config.ClusterCheck() {
		return &rpc.AddIPToGroupResponse{Success: false, Message: "no cluster configured"}, nil
	}

	// Check if group exists
	if _, exists := s.config.Groups[req.GroupName]; !exists {
		return &rpc.AddIPToGroupResponse{
			Success: false,
			Message: fmt.Sprintf("group %s does not exist", req.GroupName),
		}, nil
	}

	// Validate IP address and ensure it has a subnet mask
	ipToUse := req.Ip
	var warnings []string

	// Determine active-passive gating context
	activePassive := s.config.Pulse.Mode == "active-passive"
	activeID := ""
	if activePassive {
		for id, m := range s.memberList.Members {
			if m.Status == membership.StatusActive {
				activeID = id
				break
			}
		}
		if activeID == "" {
			warnings = append(warnings, "No active node currently; IP will be enforced when a node becomes active")
		}
	}

	// Check if it's already in CIDR notation
	if !utils.IsCIDR(req.Ip) {
		if utils.IsIPv4(req.Ip) {
			ipToUse = req.Ip + "/32" // Default to single host for IPv4
			warnings = append(warnings, fmt.Sprintf("No subnet mask provided, using %s", ipToUse))
		} else if utils.IsIPv6(req.Ip) {
			ipToUse = req.Ip + "/128" // Default to single host for IPv6
			warnings = append(warnings, fmt.Sprintf("No subnet mask provided, using %s", ipToUse))
		} else {
			return &rpc.AddIPToGroupResponse{
				Success: false,
				Message: fmt.Sprintf("invalid IP address: %s", req.Ip),
			}, nil
		}
	}

	// Check if IP already exists in configuration
	alreadyInSameGroup := false
	for g, ips := range s.config.Groups {
		for _, existingIP := range ips {
			if existingIP == ipToUse {
				if g == req.GroupName {
					alreadyInSameGroup = true
					break
				}
				return &rpc.AddIPToGroupResponse{
					Success: false,
					Message: fmt.Sprintf("IP %s already exists in group %s", ipToUse, g),
				}, nil
			}
		}
		if alreadyInSameGroup {
			break
		}
	}
	// If already configured in this group, treat as idempotent success without touching interfaces
	if alreadyInSameGroup {
		s.logger.Infof("IP %s already configured in group %s; treating as success", ipToUse, req.GroupName)
		return &rpc.AddIPToGroupResponse{
			Success:  true,
			Message:  fmt.Sprintf("IP %s already exists in group %s", ipToUse, req.GroupName),
			Warnings: warnings,
		}, nil
	}

	// Find nodes that have this group assigned and try to bring up the IP
	ipBroughtUp := false
	for nodeID, node := range s.config.Nodes {
		for iface, groups := range node.IPGroups {
			for _, g := range groups {
				if g == req.GroupName {
					// In active-passive mode, only enforce on the current active node
					if activePassive && activeID != "" && nodeID != activeID {
						// Skip bringing IP up on passive nodes; config still records the IP
						continue
					}
					// Check if this is the local node
					if nodeID == s.config.Pulse.LocalNode {
						// This is the local node, bring up the IP locally
						s.logger.Infof("Bringing up IP %s on interface %s", ipToUse, iface)

						// Check if interface exists
						exists, _ := network.InterfaceExist(iface)
						if !exists {
							warnings = append(warnings, fmt.Sprintf("Interface %s does not exist on local node", iface))
							continue
						}

						// Check if IP is already present; treat as success if on target iface
						ipObj, _ := utils.GetCIDR(ipToUse)
						if ipObj != nil {
							exists, existingIface, err := network.CheckIfIPExists(ipObj.String())
							if err != nil {
								warnings = append(warnings, fmt.Sprintf("Failed to check if IP exists: %v", err))
								continue
							}
							if exists {
								if existingIface == iface {
									// Already configured on desired iface; mark success and update expected IPs
									ipBroughtUp = true
									s.logger.Infof("IP %s already present on interface %s; treating as success", ipToUse, iface)
									if s.ipMonitor != nil {
										s.ipMonitor.UpdateExpectedIPs(iface, []string{ipToUse})
									}
									continue
								}
								// Present on a different iface; try to bring it down there first
								if derr := network.BringIPdown(existingIface, ipToUse); derr != nil {
									warnings = append(warnings, fmt.Sprintf("Failed to remove existing IP %s from interface %s: %v", ipToUse, existingIface, derr))
									continue
								}
							}
						}

						if err := network.BringIPup(iface, ipToUse); err != nil {
							warnings = append(warnings, fmt.Sprintf("Failed to bring up IP %s on interface %s: %v", ipToUse, iface, err))
							continue
						}
						ipBroughtUp = true
						s.logger.Infof("Successfully brought up IP %s on interface %s", ipToUse, iface)
						// Update expected IPs for local monitor
						if s.ipMonitor != nil {
							s.ipMonitor.UpdateExpectedIPs(iface, []string{ipToUse})
						}
					} else {
						// This is a remote node, send RPC to bring up the IP
						s.logger.Infof("Sending request to bring up IP %s on node %s", ipToUse, node.Hostname)
						remoteClient, err := client.New()
						if err != nil {
							warnings = append(warnings, fmt.Sprintf("Failed to create client for node %s: %v", node.Hostname, err))
							continue
						}

						// Connect to remote node
						if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
							remoteClient.Close()
							warnings = append(warnings, fmt.Sprintf("Failed to connect to node %s: %v", node.Hostname, err))
							continue
						}

						// Send request to bring up IP
						resp, err := remoteClient.Server().BringUpIP(ctx, &rpc.UpIpRequest{
							Iface: iface,
							Ips:   []string{ipToUse},
						})
						remoteClient.Close()

						if err != nil {
							warnings = append(warnings, fmt.Sprintf("Failed to bring up IP %s on node %s: %v", ipToUse, node.Hostname, err))
							continue
						}

						if !resp.Success {
							warnings = append(warnings, fmt.Sprintf("Failed to bring up IP %s on node %s: %s", ipToUse, node.Hostname, resp.Message))
							continue
						}

						ipBroughtUp = true
						s.logger.Infof("Successfully brought up IP %s on node %s", ipToUse, node.Hostname)
					}
				}
			}
		}
	}

	// If we couldn't bring up the IP immediately, decide whether to treat as fatal
	if !ipBroughtUp && len(warnings) > 0 {
		if activePassive {
			// In active-passive mode, lack of immediate bring-up may be expected (no active yet or gated)
			s.logger.Info("IP not brought up immediately due to active-passive gating or no active present", "ip", ipToUse)
		} else {
			return &rpc.AddIPToGroupResponse{
				Success:  false,
				Message:  "Failed to bring up IP on any node",
				Warnings: warnings,
			}, nil
		}
	}

	// Add IP to group in config
	s.config.Groups[req.GroupName] = append(s.config.Groups[req.GroupName], ipToUse)

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.AddIPToGroupResponse{
			Success:  false,
			Message:  fmt.Sprintf("failed to save config: %v", err),
			Warnings: warnings,
		}, nil
	}

	s.logger.Infof("Successfully added IP %s to group %s", ipToUse, req.GroupName)
	return &rpc.AddIPToGroupResponse{
		Success:  true,
		Message:  fmt.Sprintf("successfully added IP %s to group %s", ipToUse, req.GroupName),
		Warnings: warnings,
	}, nil
}

// RemoveIPFromGroup implements the CLI.RemoveIPFromGroup RPC method
func (s *Server) RemoveIPFromGroup(ctx context.Context, req *rpc.RemoveIPFromGroupRequest) (*rpc.RemoveIPFromGroupResponse, error) {
	s.logger.Infof("Received RemoveIPFromGroup request for group: %s, IP: %s", req.GroupName, req.Ip)
	s.Lock()
	defer s.Unlock()

	if !s.config.ClusterCheck() {
		return &rpc.RemoveIPFromGroupResponse{Success: false, Message: "no cluster configured"}, nil
	}

	// Check if group exists
	group, exists := s.config.Groups[req.GroupName]
	if !exists {
		return &rpc.RemoveIPFromGroupResponse{
			Success: false,
			Message: fmt.Sprintf("group %s does not exist", req.GroupName),
		}, nil
	}

	// Validate IP address and ensure it has a subnet mask
	ipToUse := req.Ip
	var warnings []string

	// Check if it's already in CIDR notation
	if !utils.IsCIDR(req.Ip) {
		if utils.IsIPv4(req.Ip) {
			ipToUse = req.Ip + "/32" // Default to single host for IPv4
			warnings = append(warnings, fmt.Sprintf("No subnet mask provided, using %s", ipToUse))
		} else if utils.IsIPv6(req.Ip) {
			ipToUse = req.Ip + "/128" // Default to single host for IPv6
			warnings = append(warnings, fmt.Sprintf("No subnet mask provided, using %s", ipToUse))
		} else {
			return &rpc.RemoveIPFromGroupResponse{
				Success: false,
				Message: fmt.Sprintf("invalid IP address: %s", req.Ip),
			}, nil
		}
	}

	// Find and remove IP from group
	found := false
	var newIPs []string
	var foundExactIP string
	for _, existingIP := range group {
		if existingIP == ipToUse {
			found = true
			foundExactIP = existingIP
			continue
		}
		newIPs = append(newIPs, existingIP)
	}

	if !found {
		s.logger.Infof("IP %s not present in group %s; treating as success", ipToUse, req.GroupName)
		return &rpc.RemoveIPFromGroupResponse{
			Success: true,
			Message: fmt.Sprintf("IP %s not found in group %s", ipToUse, req.GroupName),
		}, nil
	}

	// Find nodes that have this group assigned and bring down the IP
	ipBroughtDown := false
	for nodeID, node := range s.config.Nodes {
		for iface, groups := range node.IPGroups {
			for _, g := range groups {
				if g == req.GroupName {
					// Check if this is the local node
					if nodeID == s.config.Pulse.LocalNode {
						// This is the local node, bring down the IP locally
						s.logger.Infof("Bringing down IP %s on interface %s", foundExactIP, iface)

						// Check if interface exists
						exists, _ := network.InterfaceExist(iface)
						if !exists {
							warnings = append(warnings, fmt.Sprintf("Interface %s does not exist on local node", iface))
							continue
						}

						if err := network.BringIPdown(iface, foundExactIP); err != nil {
							warnings = append(warnings, fmt.Sprintf("Failed to bring down IP %s on interface %s: %v", foundExactIP, iface, err))
							// Continue anyway, as we want to remove the IP from config
						} else {
							ipBroughtDown = true
							s.logger.Infof("Successfully brought down IP %s on interface %s", foundExactIP, iface)
							if s.ipMonitor != nil {
								s.ipMonitor.RemoveExpectedIPs(iface, []string{foundExactIP})
							}
						}
					} else {
						// This is a remote node, send RPC to bring down the IP
						s.logger.Infof("Sending request to bring down IP %s on node %s", foundExactIP, node.Hostname)
						remoteClient, err := client.New()
						if err != nil {
							warnings = append(warnings, fmt.Sprintf("Failed to create client for node %s: %v", node.Hostname, err))
							continue
						}

						// Connect to remote node
						if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
							remoteClient.Close()
							warnings = append(warnings, fmt.Sprintf("Failed to connect to node %s: %v", node.Hostname, err))
							continue
						}

						// Send request to bring down IP
						resp, err := remoteClient.Server().BringDownIP(ctx, &rpc.DownIpRequest{
							Iface: iface,
							Ips:   []string{foundExactIP},
						})
						remoteClient.Close()

						if err != nil {
							warnings = append(warnings, fmt.Sprintf("Failed to bring down IP %s on node %s: %v", foundExactIP, node.Hostname, err))
							// Continue anyway, as we want to remove the IP from config
						} else if !resp.Success {
							warnings = append(warnings, fmt.Sprintf("Failed to bring down IP %s on node %s: %s", foundExactIP, node.Hostname, resp.Message))
							// Continue anyway, as we want to remove the IP from config
						} else {
							ipBroughtDown = true
							s.logger.Infof("Successfully brought down IP %s on node %s", foundExactIP, node.Hostname)
						}
					}
				}
			}
		}
	}

	// Update group in config - ensure we always use an empty slice instead of null
	if newIPs == nil {
		newIPs = make([]string, 0)
	}
	s.config.Groups[req.GroupName] = newIPs

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.RemoveIPFromGroupResponse{
			Success:  false,
			Message:  fmt.Sprintf("failed to save config: %v", err),
			Warnings: warnings,
		}, nil
	}

	// If we couldn't bring down the IP on any node but it was in the config, add a warning
	if !ipBroughtDown && len(warnings) > 0 {
		warnings = append(warnings, "IP was removed from configuration but could not be brought down on any node. You may need to manually remove the IP from interfaces if it's still active.")
	}

	s.logger.Infof("Successfully removed IP %s from group %s", ipToUse, req.GroupName)
	return &rpc.RemoveIPFromGroupResponse{
		Success:  true,
		Message:  fmt.Sprintf("successfully removed IP %s from group %s", ipToUse, req.GroupName),
		Warnings: warnings,
	}, nil
}

// AssignGroupToNode implements the CLI.AssignGroupToNode RPC method
func (s *Server) AssignGroupToNode(ctx context.Context, req *rpc.AssignGroupRequest) (*rpc.AssignGroupResponse, error) {
	s.logger.Infof("Received AssignGroupToNode request for group: %s", req.GroupName)
	s.Lock()
	defer s.Unlock()

	if !s.config.ClusterCheck() {
		return &rpc.AssignGroupResponse{Success: false, Message: "no cluster configured"}, nil
	}

	// Validate group
	if _, exists := s.config.Groups[req.GroupName]; !exists {
		return &rpc.AssignGroupResponse{Success: false, Message: fmt.Sprintf("group %s does not exist", req.GroupName)}, nil
	}

	// Find node by node ID
	var nodeFound bool
	var node *config.Node
	if n, ok := s.config.Nodes[req.NodeId]; ok {
		nodeFound = true
		node = n
	}

	if !nodeFound || node == nil {
		return &rpc.AssignGroupResponse{
			Success: false,
			Message: fmt.Sprintf("node_id %s not found", req.NodeId),
		}, nil
	}

	// Initialize IPGroups map if needed
	if node.IPGroups == nil {
		node.IPGroups = make(map[string][]string)
	}

	// Check if group is already assigned to this interface (idempotent success)
	for _, g := range node.IPGroups[req.Interface] {
		if g == req.GroupName {
			s.logger.Infof("Group %s already assigned to %s on node %s; treating as success", req.GroupName, req.Interface, req.NodeId)
			return &rpc.AssignGroupResponse{
				Success: true,
				Message: fmt.Sprintf("group %s already assigned to interface %s on node %s", req.GroupName, req.Interface, req.NodeId),
			}, nil
		}
	}

	// Add group to interface
	node.IPGroups[req.Interface] = append(node.IPGroups[req.Interface], req.GroupName)

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.AssignGroupResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	// If assigning on the local node, refresh expected IPs for this interface
	if s.ipMonitor != nil {
		if localID, err := s.config.GetLocalNodeUUID(); err == nil && req.NodeId == localID {
			node := s.config.Nodes[localID]
			if node != nil {
				iface := req.Interface
				var ifaceIPs []string
				for _, g := range node.IPGroups[iface] {
					if ips, ok := s.config.Groups[g]; ok {
						ifaceIPs = append(ifaceIPs, ips...)
					}
				}
				s.ipMonitor.ClearExpectedIPs(iface)
				if len(ifaceIPs) > 0 {
					s.ipMonitor.UpdateExpectedIPs(iface, ifaceIPs)
				}
			}
		}
	}

	s.logger.Infof("Successfully assigned group %s to interface %s on node %s", req.GroupName, req.Interface, req.NodeId)
	return &rpc.AssignGroupResponse{
		Success: true,
		Message: fmt.Sprintf("successfully assigned group %s to interface %s on node %s", req.GroupName, req.Interface, req.NodeId),
	}, nil
}

// Temporary structs for new RPC methods (until protobuf is regenerated)
type UnassignGroupRequest struct {
	GroupName string
	NodeID    string
	Interface string
}

type UnassignGroupResponse struct {
	Success bool
	Message string
}

type DeleteGroupRequest struct {
	GroupName string
	Force     bool
}

type DeleteGroupResponse struct {
	Success  bool
	Message  string
	Warnings []string
}

// UnassignGroupFromNode implements the CLI.UnassignGroupFromNode RPC method
func (s *Server) UnassignGroupFromNode(ctx context.Context, req *rpc.UnassignGroupRequest) (*rpc.UnassignGroupResponse, error) {
	s.logger.Infof("Received UnassignGroupFromNode request for group: %s", req.GroupName)
	s.Lock()
	defer s.Unlock()

	if !s.config.ClusterCheck() {
		return &rpc.UnassignGroupResponse{Success: false, Message: "no cluster configured"}, nil
	}

	// Validate group
	if _, exists := s.config.Groups[req.GroupName]; !exists {
		return &rpc.UnassignGroupResponse{Success: false, Message: fmt.Sprintf("group %s does not exist", req.GroupName)}, nil
	}

	// Enforce node_id-only lookup
	if req.NodeId == "" {
		return &rpc.UnassignGroupResponse{Success: false, Message: "missing node_id"}, nil
	}
	node, exists := s.config.Nodes[req.NodeId]
	if !exists || node == nil {
		return &rpc.UnassignGroupResponse{Success: false, Message: fmt.Sprintf("node_id %s not found", req.NodeId)}, nil
	}

	// Check if group is assigned to this interface
	if node.IPGroups == nil {
		// Nothing assigned; idempotent success
		s.logger.Infof("Group %s not assigned on %s for node %s; treating as success", req.GroupName, req.Interface, req.NodeId)
		return &rpc.UnassignGroupResponse{Success: true, Message: fmt.Sprintf("group %s is not assigned to interface %s on node %s", req.GroupName, req.Interface, req.NodeId)}, nil
	}

	// Find and remove the group from interface
	groups := node.IPGroups[req.Interface]
	groupIndex := -1
	for i, g := range groups {
		if g == req.GroupName {
			groupIndex = i
			break
		}
	}

	if groupIndex == -1 {
		// Already unassigned; idempotent success
		s.logger.Infof("Group %s already unassigned from %s on node %s; treating as success", req.GroupName, req.Interface, req.NodeId)
		return &rpc.UnassignGroupResponse{Success: true, Message: fmt.Sprintf("group %s is not assigned to interface %s on node %s", req.GroupName, req.Interface, req.NodeId)}, nil
	}

	// Remove group from slice
	node.IPGroups[req.Interface] = append(groups[:groupIndex], groups[groupIndex+1:]...)

	// If interface has no more groups, remove the entry
	if len(node.IPGroups[req.Interface]) == 0 {
		delete(node.IPGroups, req.Interface)
	}

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.UnassignGroupResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	// If unassigning on the local node, refresh expected IPs for this interface
	if s.ipMonitor != nil {
		if localID, err := s.config.GetLocalNodeUUID(); err == nil && req.NodeId == localID {
			node := s.config.Nodes[localID]
			if node != nil {
				iface := req.Interface
				var ifaceIPs []string
				for _, g := range node.IPGroups[iface] {
					if ips, ok := s.config.Groups[g]; ok {
						ifaceIPs = append(ifaceIPs, ips...)
					}
				}
				s.ipMonitor.ClearExpectedIPs(iface)
				if len(ifaceIPs) > 0 {
					s.ipMonitor.UpdateExpectedIPs(iface, ifaceIPs)
				}
			}
		}
	}

	s.logger.Infof("Successfully unassigned group %s from interface %s on node %s", req.GroupName, req.Interface, req.NodeId)
	return &rpc.UnassignGroupResponse{
		Success: true,
		Message: fmt.Sprintf("successfully unassigned group %s from interface %s on node %s", req.GroupName, req.Interface, req.NodeId),
	}, nil
}

// DeleteGroup implements the CLI.DeleteGroup RPC method
func (s *Server) DeleteGroup(ctx context.Context, req *rpc.DeleteGroupRequest) (*rpc.DeleteGroupResponse, error) {
	s.logger.Infof("Received DeleteGroup request for group: %s", req.GroupName)
	s.Lock()
	defer s.Unlock()

	if !s.config.ClusterCheck() {
		return &rpc.DeleteGroupResponse{Success: false, Message: "no cluster configured"}, nil
	}

	// Validate group exists (idempotent success if missing)
	if _, exists := s.config.Groups[req.GroupName]; !exists {
		s.logger.Infof("Group %s does not exist; treating delete as success", req.GroupName)
		return &rpc.DeleteGroupResponse{Success: true, Message: fmt.Sprintf("group %s does not exist", req.GroupName)}, nil
	}

	// Check if group is assigned to any nodes (unless force is true)
	var assignedNodes []string
	for _, node := range s.config.Nodes {
		for iface, groups := range node.IPGroups {
			for _, group := range groups {
				if group == req.GroupName {
					assignedNodes = append(assignedNodes, fmt.Sprintf("%s:%s", node.Hostname, iface))
				}
			}
		}
	}

	if len(assignedNodes) > 0 && !req.Force {
		return &rpc.DeleteGroupResponse{
			Success: false,
			Message: fmt.Sprintf("group %s is assigned to nodes: %s. Use --force to delete anyway", req.GroupName, assignedNodes),
		}, nil
	}

	// If force is true and group is assigned, remove assignments and add warnings
	var warnings []string
	if len(assignedNodes) > 0 && req.Force {
		for _, node := range s.config.Nodes {
			for iface := range node.IPGroups {
				groups := node.IPGroups[iface]
				for i := len(groups) - 1; i >= 0; i-- {
					if groups[i] == req.GroupName {
						// Remove group from slice
						node.IPGroups[iface] = append(groups[:i], groups[i+1:]...)
						warnings = append(warnings, fmt.Sprintf("removed assignment from %s:%s", node.Hostname, iface))
					}
				}
				// If interface has no more groups, remove the entry
				if len(node.IPGroups[iface]) == 0 {
					delete(node.IPGroups, iface)
				}
			}
		}
	}

	// Delete the group
	delete(s.config.Groups, req.GroupName)

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.DeleteGroupResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	s.logger.Infof("Successfully deleted group %s", req.GroupName)
	return &rpc.DeleteGroupResponse{
		Success:  true,
		Message:  fmt.Sprintf("successfully deleted group %s", req.GroupName),
		Warnings: warnings,
	}, nil
}

// ListGroups implements the CLI.ListGroups RPC method
func (s *Server) ListGroups(ctx context.Context, req *rpc.ListGroupsRequest) (*rpc.ListGroupsResponse, error) {
	s.logger.Info("Received ListGroups request")
	s.RLock()
	defer s.RUnlock()

	if len(s.config.Groups) == 0 {
		return &rpc.ListGroupsResponse{
			Success: true,
			Message: "no IP groups configured",
			Groups:  []*rpc.GroupInfo{},
		}, nil
	}

	// If JSON output is requested, marshal the groups
	if req.JsonOutput {
		jsonData, err := json.MarshalIndent(s.config.Groups, "", "  ")
		if err != nil {
			s.logger.Error("Failed to marshal groups", "error", err)
			return &rpc.ListGroupsResponse{
				Success: false,
				Message: fmt.Sprintf("failed to marshal groups: %v", err),
			}, nil
		}
		return &rpc.ListGroupsResponse{
			Success:  true,
			Message:  "groups retrieved successfully",
			JsonData: string(jsonData),
		}, nil
	}

	// Otherwise, build structured response
	var groups []*rpc.GroupInfo
	for groupName, ips := range s.config.Groups {
		group := &rpc.GroupInfo{
			Name: groupName,
			Ips:  ips,
		}

		// Find assignments for this group
		for id, node := range s.config.Nodes {
			for iface, assignedGroups := range node.IPGroups {
				for _, g := range assignedGroups {
					if g == groupName {
						group.Assignments = append(group.Assignments, &rpc.GroupAssignment{
							Interface: iface,
							NodeId:    id,
						})
					}
				}
			}
		}

		groups = append(groups, group)
	}

	s.logger.Infof("Successfully retrieved %d groups", len(groups))
	return &rpc.ListGroupsResponse{
		Success: true,
		Message: "groups retrieved successfully",
		Groups:  groups,
	}, nil
}

// CreateCluster implements the CLI.CreateCluster RPC method
func (s *Server) CreateCluster(ctx context.Context, req *rpc.CreateClusterRequest) (*rpc.CreateClusterResponse, error) {
	s.logger.Infof("Received CreateCluster request with bindIP: %s, bindPort: %s, mode: %s", req.BindIp, req.BindPort, req.Mode)

	// Check if cluster is already configured
	if s.config.ClusterCheck() {
		return &rpc.CreateClusterResponse{
			Success: false,
			Message: "cluster is already configured",
		}, nil
	}

	// Set up initial node
	bindPort := req.BindPort
	if bindPort == "" {
		bindPort = "8080"
	}

	// Get hostname for certificates
	hostname, err := os.Hostname()
	if err != nil {
		s.logger.Error("Failed to get hostname", "error", err)
		return &rpc.CreateClusterResponse{
			Success: false,
			Message: fmt.Sprintf("failed to get hostname: %v", err),
		}, nil
	}

	// Generate certificates for mTLS
	if os.Getenv("PULSEHA_TEST") != "true" {
		if err := security.GenerateCertificates(hostname); err != nil {
			s.logger.Warn("Failed to generate certificates", "error", err)
			// Continue without TLS for now
		}
	} else {
		s.logger.Debug("PULSEHA_TEST=true: skipping certificate generation in CreateCluster")
	}

	// Generate a unique node ID
	nodeID := req.NodeId
	if nodeID == "" {
		nodeID = s.config.GenerateNodeID()
		s.logger.Infof("Generated node ID: %s", nodeID)
	} else {
		s.logger.Infof("Using provided node ID: %s", nodeID)
	}

	// Generate a cluster token for other nodes to join
	clusterToken := uuid.New().String()
	s.config.Pulse.ClusterToken = clusterToken
	s.logger.Infof("Generated cluster token: %s", clusterToken)

	// Add the node to config using generated ID
	s.config.Nodes[nodeID] = &config.Node{
		Hostname: hostname,
		IP:       req.BindIp,
		Port:     bindPort,
		IPGroups: make(map[string][]string),
	}

	// Set local node to the generated ID
	s.config.Pulse.LocalNode = nodeID

	// Set the cluster mode
	s.config.Pulse.Mode = req.Mode

	// Create default IP groups for each network interface
	interfaces, err := net.Interfaces()
	if err != nil {
		s.logger.Warn("Failed to get network interfaces", "error", err)
	} else {
		for _, iface := range interfaces {
			// Skip loopback, down interfaces, and interfaces without addresses
			if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
				continue
			}

			addrs, err := iface.Addrs()
			if err != nil {
				s.logger.Warn("Failed to get addresses for interface %s", "interface", iface.Name, "error", err)
				continue
			}

			if len(addrs) == 0 {
				continue
			}

			// Create a group for this interface
			groupName := fmt.Sprintf("default-%s", iface.Name)

			// Initialize the group if it doesn't exist
			if _, exists := s.config.Groups[groupName]; !exists {
				s.config.Groups[groupName] = []string{}
				s.logger.Infof("Created default IP group for interface %s", iface.Name)

				// Assign this group to the node's interface
				if s.config.Nodes[nodeID].IPGroups == nil {
					s.config.Nodes[nodeID].IPGroups = make(map[string][]string)
				}
				s.config.Nodes[nodeID].IPGroups[iface.Name] = append(s.config.Nodes[nodeID].IPGroups[iface.Name], groupName)
				s.logger.Infof("Assigned default IP group %s to interface %s on node %s", groupName, iface.Name, hostname)

				// Ensure monitor has a clean slate for this interface
				if s.ipMonitor != nil {
					s.ipMonitor.ClearExpectedIPs(iface.Name)
				}
			}
		}
	}

	// Save config
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save config", "error", err)
		return &rpc.CreateClusterResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save config: %v", err),
		}, nil
	}

	// Add the first member to the member list
	if err := s.memberList.AddMember(nodeID, hostname, req.BindIp, bindPort); err != nil {
		s.logger.Error("Failed to add first node to member list", "error", err)
		return &rpc.CreateClusterResponse{
			Success: false,
			Message: fmt.Sprintf("failed to add first node to member list: %v", err),
		}, nil
	}

	// Make it active
	member := s.memberList.GetMemberByID(nodeID)
	if member != nil {
		member.Status = membership.StatusActive
		s.logger.Info("First node activated successfully")
	}

	// Reconfigure the server to apply changes
	if err := s.Reconfigure(); err != nil {
		s.logger.Error("Failed to reconfigure server", "error", err)
		return &rpc.CreateClusterResponse{
			Success: false,
			Message: fmt.Sprintf("cluster created but failed to reconfigure server: %v", err),
		}, nil
	}

	// After successfully creating the cluster, start the health checker
	s.startHealthChecker()

	// Best-effort: wait briefly for the cluster listener to be ready to accept connections
	// This improves UX by ensuring the service is reachable immediately after successful creation
	finalPort := bindPort
	if bindPort == "0" {
		// Resolve actual bound port from config after Reconfigure
		if localNode, e := s.config.GetLocalNode(); e == nil {
			finalPort = localNode.Port
		}
	}
	address := fmt.Sprintf("%s:%s", req.BindIp, finalPort)
	readyDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(readyDeadline) {
		conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	s.logger.Info("Cluster created successfully")
	return &rpc.CreateClusterResponse{
		Success: true,
		Message: "cluster created successfully",
		Token:   clusterToken,
		NodeId:  nodeID,
	}, nil
}

// Token implements the CLI.Token RPC method
func (s *Server) Token(ctx context.Context, req *rpc.TokenRequest) (*rpc.TokenResponse, error) {
	s.logger.Infof("Received Token request with regenerate: %t", req.Regenerate)
	s.Lock()
	defer s.Unlock()

	// Check if cluster is configured
	if !s.config.ClusterCheck() {
		return &rpc.TokenResponse{
			Success: false,
			Message: "no cluster configured",
		}, nil
	}

	currentToken := s.config.Pulse.ClusterToken

	// If regenerate is false, just return the current token
	if !req.Regenerate {
		if currentToken == "" {
			return &rpc.TokenResponse{
				Success: false,
				Message: "no cluster token available",
			}, nil
		}
		return &rpc.TokenResponse{
			Success: true,
			Message: "current cluster token",
			Token:   currentToken,
		}, nil
	}

	// Generate new token
	newToken := uuid.New().String()
	if newToken == "" {
		return &rpc.TokenResponse{
			Success: false,
			Message: "failed to generate new token",
		}, nil
	}

	// Update the config
	s.config.Pulse.ClusterToken = newToken

	// TODO: Implement cluster-wide config synchronization
	// For now, the token will be updated on this node only
	// Future enhancement: sync with all cluster members

	// Save the configuration
	if err := s.config.Save(); err != nil {
		s.logger.Error("Failed to save configuration with new token", "error", err)
		return &rpc.TokenResponse{
			Success: false,
			Message: fmt.Sprintf("failed to save new token: %v", err),
		}, nil
	}

	// Best-effort: broadcast updated token via ConfigSync to all peers
	configBytes, err := json.Marshal(s.config)
	if err == nil {
		localID, _ := s.config.GetLocalNodeUUID()
		for id, node := range s.config.Nodes {
			if id == localID {
				continue
			}
			remoteClient, err := client.New()
			if err != nil {
				continue
			}
			if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
				remoteClient.Close()
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = remoteClient.Server().ConfigSync(ctx, &rpc.ConfigSyncRequest{Config: configBytes})
			cancel()
			remoteClient.Close()
		}
	}

	s.logger.Infof("Successfully generated new cluster token")
	return &rpc.TokenResponse{
		Success: true,
		Message: "new cluster token generated",
		Token:   newToken,
	}, nil
}

// Quorum-related RPC method implementations that delegate to the quorum handler

// StartVotingSession delegates to the quorum handler
func (s *Server) StartVotingSession(ctx context.Context, req *rpc.StartVotingSessionRequest) (*rpc.StartVotingSessionResponse, error) {
	if s.quorumHandler == nil {
		return &rpc.StartVotingSessionResponse{
			Success: false,
			Message: "Quorum voting is not available",
		}, fmt.Errorf("quorum handler is not initialized")
	}
	return s.quorumHandler.StartVotingSession(ctx, req)
}

// CastVote delegates to the quorum handler
func (s *Server) CastVote(ctx context.Context, req *rpc.CastVoteRequest) (*rpc.CastVoteResponse, error) {
	if s.quorumHandler == nil {
		return &rpc.CastVoteResponse{
			Success: false,
			Message: "Quorum voting is not available",
		}, fmt.Errorf("quorum handler is not initialized")
	}
	return s.quorumHandler.CastVote(ctx, req)
}

// GetVotingResult delegates to the quorum handler
func (s *Server) GetVotingResult(ctx context.Context, req *rpc.GetVotingResultRequest) (*rpc.GetVotingResultResponse, error) {
	if s.quorumHandler == nil {
		return &rpc.GetVotingResultResponse{
			Success: false,
			Message: "Quorum voting is not available",
		}, fmt.Errorf("quorum handler is not initialized")
	}
	return s.quorumHandler.GetVotingResult(ctx, req)
}

// GetVotingSessions delegates to the quorum handler
func (s *Server) GetVotingSessions(ctx context.Context, req *rpc.GetVotingSessionsRequest) (*rpc.GetVotingSessionsResponse, error) {
	if s.quorumHandler == nil {
		return &rpc.GetVotingSessionsResponse{
			Success: false,
			Message: "Quorum voting is not available",
		}, fmt.Errorf("quorum handler is not initialized")
	}
	return s.quorumHandler.GetVotingSessions(ctx, req)
}

// GetVotingSessionDetails delegates to the quorum handler
func (s *Server) GetVotingSessionDetails(ctx context.Context, req *rpc.GetVotingSessionDetailsRequest) (*rpc.GetVotingSessionDetailsResponse, error) {
	if s.quorumHandler == nil {
		return &rpc.GetVotingSessionDetailsResponse{
			Success: false,
			Message: "Quorum voting is not available",
		}, fmt.Errorf("quorum handler is not initialized")
	}
	return s.quorumHandler.GetVotingSessionDetails(ctx, req)
}

// GetQuorumManager returns the quorum manager instance
func (s *Server) GetQuorumManager() *quorum.QuorumManager {
	return s.quorumManager
}

// ConfigSync handles configuration synchronization between nodes
func (s *Server) ConfigSync(ctx context.Context, req *rpc.ConfigSyncRequest) (*rpc.ConfigSyncResponse, error) {
	s.logger.Debug("Received configuration sync request")

	if req.Config == nil {
		return &rpc.ConfigSyncResponse{
			Success: false,
			Message: "no configuration data provided",
		}, nil
	}

	// Detect whether the incoming payload contains a full config (has "pulseha" root) or is an envelope
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(req.Config, &raw)
	isFullConfig := false
	if raw != nil {
		if _, ok := raw["pulseha"]; ok {
			isFullConfig = true
		}
	}

	// Optionally read member states and convergence metadata if present (EnhancedConfig)
	var (
		incomingMemberStates map[string]membership.MemberStatus
		incomingEpoch        int64
		incomingLeaderID     string
	)
	{
		type enhanced struct {
			MemberStates map[string]int    `json:"member_states"`
			Epoch        *int64            `json:"epoch"`
			LeaderID     string            `json:"leader_id"`
			Leases       map[string]string `json:"leases"`
		}
		var e enhanced
		if err := json.Unmarshal(req.Config, &e); err == nil {
			if len(e.MemberStates) > 0 {
				incomingMemberStates = make(map[string]membership.MemberStatus, len(e.MemberStates))
				for id, st := range e.MemberStates {
					incomingMemberStates[id] = membership.MemberStatus(st)
				}
			}
			if e.Epoch != nil {
				incomingEpoch = *e.Epoch
			}
			incomingLeaderID = e.LeaderID
		}
	}

	if isFullConfig {
		// Create a new config instance to hold incoming cluster-wide configuration
		newConfig := &config.Config{}

		// Unmarshal the received configuration
		if err := json.Unmarshal(req.Config, newConfig); err != nil {
			s.logger.Error("Failed to unmarshal configuration", "error", err)
			return &rpc.ConfigSyncResponse{
				Success: false,
				Message: fmt.Sprintf("failed to unmarshal configuration: %v", err),
			}, nil
		}

		// Preserve the local node identity from our existing configuration to avoid adopting remote LocalNode
		prevLocalID := s.config.Pulse.LocalNode
		if prevLocalID != "" && newConfig.Pulse.LocalNode != prevLocalID {
			s.logger.Debugf("ConfigSync: preserving local node identity: %s (incoming had %s)", prevLocalID, newConfig.Pulse.LocalNode)
			newConfig.Pulse.LocalNode = prevLocalID
			// Ensure the node entry exists in incoming config
			if newConfig.Nodes == nil {
				newConfig.Nodes = map[string]*config.Node{}
			}
			if _, ok := newConfig.Nodes[prevLocalID]; !ok {
				if existing := s.config.Nodes[prevLocalID]; existing != nil {
					// Shallow copy to avoid aliasing
					copied := *existing
					newConfig.Nodes[prevLocalID] = &copied
				}
			}
		}

		// Preserve local-specific settings before applying cluster config
		// These should not be overwritten by a remote ConfigSync
		localIDPreserve := s.config.Pulse.LocalNode
		loggingLevelPreserve := s.config.Pulse.LoggingLevel
		logToFilePreserve := s.config.Pulse.LogToFile
		logFileLocationPreserve := s.config.Pulse.LogFileLocation
		logToSyslogPreserve := s.config.Pulse.LogToSyslog
		syslogNetworkPreserve := s.config.Pulse.SyslogNetwork
		syslogAddressPreserve := s.config.Pulse.SyslogAddress
		syslogFacilityPreserve := s.config.Pulse.SyslogFacility
		syslogTagPreserve := s.config.Pulse.SyslogTag

		s.Lock()

		// Apply preserved local-specific settings onto the incoming config
		newConfig.Pulse.LocalNode = localIDPreserve
		newConfig.Pulse.LoggingLevel = loggingLevelPreserve
		newConfig.Pulse.LogToFile = logToFilePreserve
		newConfig.Pulse.LogFileLocation = logFileLocationPreserve
		newConfig.Pulse.LogToSyslog = logToSyslogPreserve
		newConfig.Pulse.SyslogNetwork = syslogNetworkPreserve
		newConfig.Pulse.SyslogAddress = syslogAddressPreserve
		newConfig.Pulse.SyslogFacility = syslogFacilityPreserve
		newConfig.Pulse.SyslogTag = syslogTagPreserve

		// Merge: preserve/merge groups and interface assignments when missing or empty in incoming
		if len(newConfig.Groups) == 0 && len(s.config.Groups) > 0 {
			// Deep copy local groups
			newConfig.Groups = make(map[string][]string, len(s.config.Groups))
			for g, ips := range s.config.Groups {
				copySlice := make([]string, len(ips))
				copy(copySlice, ips)
				newConfig.Groups[g] = copySlice
			}
		}
		// For any group present with empty list, prefer local non-empty list
		if len(s.config.Groups) > 0 {
			if newConfig.Groups == nil {
				newConfig.Groups = make(map[string][]string)
			}
			for g, localIPs := range s.config.Groups {
				if len(localIPs) == 0 {
					continue
				}
				incomingIPs, ok := newConfig.Groups[g]
				if !ok || len(incomingIPs) == 0 {
					copySlice := make([]string, len(localIPs))
					copy(copySlice, localIPs)
					newConfig.Groups[g] = copySlice
				}
			}
		}
		// Preserve per-node interface group assignments when missing in incoming
		for nodeID, existing := range s.config.Nodes {
			if existing == nil {
				continue
			}
			nIncoming, ok := newConfig.Nodes[nodeID]
			if !ok || nIncoming == nil {
				// Keep existing node entirely if absent
				copied := *existing
				if copied.IPGroups != nil {
					copiedGroups := make(map[string][]string, len(copied.IPGroups))
					for iface, groups := range copied.IPGroups {
						gg := make([]string, len(groups))
						copy(gg, groups)
						copiedGroups[iface] = gg
					}
					copied.IPGroups = copiedGroups
				}
				newConfig.Nodes[nodeID] = &copied
				continue
			}
			if len(nIncoming.IPGroups) == 0 && len(existing.IPGroups) > 0 {
				nIncoming.IPGroups = make(map[string][]string, len(existing.IPGroups))
				for iface, groups := range existing.IPGroups {
					gg := make([]string, len(groups))
					copy(gg, groups)
					nIncoming.IPGroups[iface] = gg
				}
			}
			// For any interface present with empty group list, prefer local list
			for iface, localGroups := range existing.IPGroups {
				if len(localGroups) == 0 {
					continue
				}
				incomingGroups, ok := nIncoming.IPGroups[iface]
				if !ok || len(incomingGroups) == 0 {
					gg := make([]string, len(localGroups))
					copy(gg, localGroups)
					nIncoming.IPGroups[iface] = gg
				}
			}
		}

		// Persist and update our configuration
		if err := newConfig.Save(); err != nil {
			s.logger.Error("Failed to save synchronized configuration", "error", err)
			s.Unlock()
			return &rpc.ConfigSyncResponse{
				Success: false,
				Message: fmt.Sprintf("failed to save synchronized configuration: %v", err),
			}, nil
		}
		s.config = newConfig
		s.Unlock()

		// Update convergence metadata if newer
		if incomingEpoch > s.clusterEpoch {
			s.clusterEpoch = incomingEpoch
			s.leaderID = incomingLeaderID
		}

		// Immediately refresh member list from new configuration so peers become visible
		s.memberList.UpdateConfig(s.config)
		if err := s.loadInitialMembers(); err != nil {
			s.logger.Warn("ConfigSync: failed to load members after sync", "error", err)
		}
	} else {
		// Envelope-only update: do NOT overwrite config; just apply incoming states and metadata
		if incomingEpoch > s.clusterEpoch {
			s.clusterEpoch = incomingEpoch
			s.leaderID = incomingLeaderID
		}
		// Ensure member list is aligned with current config
		s.memberList.UpdateConfig(s.config)
		if err := s.loadInitialMembers(); err != nil {
			s.logger.Warn("ConfigSync: failed to load members after envelope sync", "error", err)
		}
	}

	// Apply incoming member states if provided
	if len(incomingMemberStates) > 0 {
		for id, st := range incomingMemberStates {
			if m := s.memberList.GetMemberByID(id); m != nil {
				m.Status = st
			}
		}
		// Enforce view based on mode
		if _, err := s.config.GetLocalNodeUUID(); err == nil {
			if s.config.Pulse.Mode == "active-passive" {
				// Enforce single active matching incoming leader/member states
				var activeID string
				for id, st := range incomingMemberStates {
					if st == membership.StatusActive {
						activeID = id
						break
					}
				}
				if activeID != "" {
					for id, m := range s.memberList.Members {
						if id == activeID {
							m.Status = membership.StatusActive
						} else {
							m.Status = membership.StatusPassive
						}
					}
				}
			} else {
				// active-active: accept multiple actives; no special enforcement beyond applying states
			}
		}
	}

	// Trigger async reconfigure
	go func() {
		if err := s.Reconfigure(); err != nil {
			s.logger.Error("Async reconfigure failed after ConfigSync", "error", err)
		} else {
			s.logger.Info("Async reconfigure completed after ConfigSync")
		}
	}()

	// Rebuild expected IPs in the IP monitor from the synchronized config (local node only)
	if s.ipMonitor != nil {
		if localID, err := s.config.GetLocalNodeUUID(); err == nil {
			if localNode := s.config.Nodes[localID]; localNode != nil {
				for iface, groupNames := range localNode.IPGroups {
					// Aggregate all IPs from the groups assigned to this interface
					var ifaceIPs []string
					for _, g := range groupNames {
						if ips, ok := s.config.Groups[g]; ok {
							ifaceIPs = append(ifaceIPs, ips...)
						}
					}
					// Reset and update expected IPs for this interface
					s.ipMonitor.ClearExpectedIPs(iface)
					if len(ifaceIPs) > 0 {
						s.ipMonitor.UpdateExpectedIPs(iface, ifaceIPs)
					}
				}
			}
		}
	}

	s.logger.Info("Configuration successfully synchronized")
	return &rpc.ConfigSyncResponse{
		Success: true,
		Message: "configuration successfully synchronized",
	}, nil
}

// AddNode adds a new node to the cluster
func (s *Server) AddNode(nodeID string) error {
	s.logger.Debugf("Adding node %s to cluster", nodeID)

	// Get node config
	node, ok := s.config.Nodes[nodeID]
	if !ok {
		s.logger.Errorf("FATAL: No configuration found for node %s", nodeID)
		return fmt.Errorf("no configuration found for node %s", nodeID)
	}

	if err := s.memberList.AddMember(nodeID, node.Hostname, node.IP, node.Port); err != nil {
		s.logger.Errorf("FATAL: Failed to add member %s: %v", nodeID, err)
		return fmt.Errorf("failed to add member %s: %v", nodeID, err)
	}

	return nil
}

// UpdateConfig implements CLI.UpdateConfig
func (s *Server) UpdateConfig(ctx context.Context, req *rpc.UpdateConfigRequest) (*rpc.UpdateConfigResponse, error) {
	s.Lock()
	defer s.Unlock()

	if req == nil || req.Key == "" {
		return &rpc.UpdateConfigResponse{Success: false, Message: "invalid request"}, nil
	}

	if err := s.config.UpdateValue(req.Key, req.Value); err != nil {
		s.logger.Errorf("Failed to update config %s: %v", req.Key, err)
		return &rpc.UpdateConfigResponse{Success: false, Message: err.Error()}, nil
	}

	// Apply runtime changes for known keys
	if req.Key == "logging_level" {
		if level, err := log.ParseLevel(req.Value); err == nil {
			s.logger.SetLevel(level)
		}
	}

	return &rpc.UpdateConfigResponse{Success: true, Message: "updated"}, nil
}

// ResyncNetwork implements CLI.ResyncNetwork RPC
func (s *Server) ResyncNetwork(ctx context.Context, req *rpc.ResyncNetworkRequest) (*rpc.ResyncNetworkResponse, error) {
	// Avoid holding the server lock while calling Reconfigure to prevent deadlocks,
	// since Reconfigure acquires the same lock internally.

	// Optionally create default groups for new interfaces
	if req.GetCreateDefaultGroups() {
		interfaces, err := net.Interfaces()
		if err != nil {
			return &rpc.ResyncNetworkResponse{Success: false, Message: err.Error()}, nil
		}
		for _, iface := range interfaces {
			if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
				continue
			}
			groupName := fmt.Sprintf("default-%s", iface.Name)
			if _, exists := s.config.Groups[groupName]; !exists {
				s.config.Groups[groupName] = []string{}
				// assign group entry on local node so UI can see mapping
				localID, err := s.config.GetLocalNodeUUID()
				if err == nil {
					node := s.config.Nodes[localID]
					if node != nil {
						if node.IPGroups == nil {
							node.IPGroups = make(map[string][]string)
						}
						node.IPGroups[iface.Name] = append(node.IPGroups[iface.Name], groupName)
					}
				}
			}
		}
		_ = s.config.Save()

		// Refresh monitor expected IPs for local node after default group creation
		if s.ipMonitor != nil {
			if localID, err := s.config.GetLocalNodeUUID(); err == nil {
				node := s.config.Nodes[localID]
				if node != nil {
					for iface := range node.IPGroups {
						// Recompute expected IPs (likely empty at creation time)
						var ifaceIPs []string
						for _, g := range node.IPGroups[iface] {
							if ips, ok := s.config.Groups[g]; ok {
								ifaceIPs = append(ifaceIPs, ips...)
							}
						}
						s.ipMonitor.ClearExpectedIPs(iface)
						if len(ifaceIPs) > 0 {
							s.ipMonitor.UpdateExpectedIPs(iface, ifaceIPs)
						}
					}
				}
			}
		}
	}

	// Force immediate activation if cluster configuration exists
	// Create a fresh config instance to ensure we read the current on-disk config
	s.config = config.New()

	if s.config.ClusterCheck() {
		// Sync member list with latest config and reload members
		s.memberList.UpdateConfig(s.config)
		if err := s.loadInitialMembers(); err != nil {
			s.logger.Errorf("Failed to load members during resync: %v", err)
			return &rpc.ResyncNetworkResponse{Success: false, Message: fmt.Sprintf("failed to load members: %v", err)}, nil
		}

		// Ensure cluster listener is bound and health checker running
		if err := s.Reconfigure(); err != nil {
			s.logger.Errorf("Failed to reconfigure server during resync: %v", err)
			return &rpc.ResyncNetworkResponse{Success: false, Message: fmt.Sprintf("failed to reconfigure server: %v", err)}, nil
		}

		s.startHealthChecker()

		// Wait briefly for the cluster listener to become ready after resync
		if localNode, err := s.config.GetLocalNode(); err == nil {
			address := fmt.Sprintf("%s:%s", localNode.IP, localNode.Port)
			readyDeadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(readyDeadline) {
				conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
				if err == nil {
					_ = conn.Close()
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		// Membership reconciliation with quorum (clusters >=3)
		clusterSize := len(s.config.Nodes)
		if clusterSize >= 3 {
			// Build presence counts for each known node based on peer snapshots
			presenceCount := make(map[string]int)
			for id := range s.config.Nodes {
				presenceCount[id] = 0
			}

			// Query each peer for its status snapshot
			for id, node := range s.config.Nodes {
				// Skip local node
				localID, _ := s.config.GetLocalNodeUUID()
				if id == localID {
					continue
				}
				remoteClient, err := client.New()
				if err != nil {
					s.logger.Warn("Resync: failed to create client for peer", "peer", id, "error", err)
					continue
				}
				if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
					remoteClient.Close()
					s.logger.Warn("Resync: failed to connect to peer", "peer", id, "error", err)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := remoteClient.CLI().Status(ctx, &rpc.StatusRequest{})
				cancel()
				remoteClient.Close()
				if err != nil || resp == nil {
					s.logger.Warn("Resync: failed to get status from peer", "peer", id, "error", err)
					continue
				}

				// Mark nodes present in this peer's view by hostname
				for knownID, knownNode := range s.config.Nodes {
					for _, m := range resp.Members {
						if m.Hostname == knownNode.Hostname {
							presenceCount[knownID]++
							break
						}
					}
				}
			}

			// Determine majority threshold
			majority := (clusterSize / 2) + 1

			// For any node missing from majority and currently Unknown locally, propose removal vote
			for id := range s.config.Nodes {
				// Skip local node
				localID, _ := s.config.GetLocalNodeUUID()
				if id == localID {
					continue
				}
				member := s.memberList.GetMemberByID(id)
				if member == nil {
					continue
				}
				if presenceCount[id] < majority && member.Status == membership.StatusUnknown {
					// Start a quorum vote to remove this member
					if s.quorumManager == nil || len(s.config.Nodes) < 3 {
						s.logger.Infof("Resync: member %s missing from majority but quorum unavailable; skipping automatic removal", id)
						continue
					}
					subject := id
					description := fmt.Sprintf("Remove node %s due to absence from majority and unknown status", id)
					sessionID, err := s.quorumManager.StartVotingSession(quorum.VoteTypeConfigChange, subject, description, 30*time.Second)
					if err != nil {
						s.logger.Warn("Resync: failed to start removal vote", "id", id, "error", err)
						continue
					}

					// Poll for result (short window)
					passed := false
					for i := 0; i < 30; i++ {
						time.Sleep(1 * time.Second)
						session, err := s.quorumManager.GetVotingSession(sessionID)
						if err != nil {
							s.logger.Warn("Resync: failed to get voting session", "sessionID", sessionID, "error", err)
							break
						}
						if session != nil && session.Result != nil {
							passed = session.Result.Passed && session.Result.QuorumMet
							break
						}
					}

					if passed {
						// Apply removal locally
						_ = s.memberList.RemoveMember(id)
						delete(s.config.Nodes, id)
						_ = s.config.Save()

						// Broadcast updated config to peers (best-effort)
						configBytes, err := json.Marshal(s.config)
						if err == nil {
							for peerID, node := range s.config.Nodes {
								if peerID == localID {
									continue
								}
								remoteClient, err := client.New()
								if err != nil {
									continue
								}
								if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
									remoteClient.Close()
									continue
								}
								ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
								_, _ = remoteClient.Server().ConfigSync(ctx, &rpc.ConfigSyncRequest{Config: configBytes})
								cancel()
								remoteClient.Close()
							}
						}
					}
				}
			}

			return &rpc.ResyncNetworkResponse{Success: true, Message: "network resynced and cluster activated"}, nil
		}

		// For clusters <3 nodes, only log; manual cleanup required
		s.logger.Info("Resync: cluster size <3; membership reconciliation requires manual removal. No automatic changes applied.")
		return &rpc.ResyncNetworkResponse{Success: true, Message: "network resynced and activated (manual membership management)"}, nil
	}

	return &rpc.ResyncNetworkResponse{Success: true, Message: "network resynced"}, nil
}

// BringUpIP implements the Server.BringUpIP RPC for remote IP assignment
func (s *Server) BringUpIP(ctx context.Context, req *rpc.UpIpRequest) (*rpc.UpIpResponse, error) {
	s.logger.Infof("RPC BringUpIP on iface %s for %d IP(s)", req.Iface, len(req.Ips))

	for _, ip := range req.Ips {
		if !utils.IsCIDR(ip) {
			if utils.IsIPv4(ip) {
				ip = ip + "/32"
			} else if utils.IsIPv6(ip) {
				ip = ip + "/128"
			} else {
				return &rpc.UpIpResponse{Success: false, Message: "invalid IP"}, nil
			}
		}

		if err := network.BringIPup(req.Iface, ip); err != nil {
			s.logger.Error("BringUpIP failed", "iface", req.Iface, "ip", ip, "error", err)
			return &rpc.UpIpResponse{Success: false, Message: err.Error()}, nil
		}
		if err := network.SendGARP(req.Iface, ip); err != nil {
			s.logger.Warn("SendGARP failed", "iface", req.Iface, "ip", ip, "error", err)
		}
	}
	return &rpc.UpIpResponse{Success: true, Message: "IPs brought up"}, nil
}

// BringDownIP implements the Server.BringDownIP RPC for remote IP removal
func (s *Server) BringDownIP(ctx context.Context, req *rpc.DownIpRequest) (*rpc.DownIpResponse, error) {
	s.logger.Infof("RPC BringDownIP on iface %s for %d IP(s)", req.Iface, len(req.Ips))

	for _, ip := range req.Ips {
		if !utils.IsCIDR(ip) {
			if utils.IsIPv4(ip) {
				ip = ip + "/32"
			} else if utils.IsIPv6(ip) {
				ip = ip + "/128"
			} else {
				return &rpc.DownIpResponse{Success: false, Message: "invalid IP"}, nil
			}
		}

		if err := network.BringIPdown(req.Iface, ip); err != nil {
			s.logger.Error("BringDownIP failed", "iface", req.Iface, "ip", ip, "error", err)
			return &rpc.DownIpResponse{Success: false, Message: err.Error()}, nil
		}
	}
	return &rpc.DownIpResponse{Success: true, Message: "IPs brought down"}, nil
}

// InitiateJoin performs a server-driven join against a target member
func (s *Server) InitiateJoin(ctx context.Context, req *rpc.InitiateJoinRequest) (*rpc.InitiateJoinResponse, error) {
	if req == nil || req.TargetHost == "" {
		return &rpc.InitiateJoinResponse{Success: false, Message: "target_host is required"}, nil
	}

	// Prevent joining if this node is already part of a cluster
	if s.config != nil && s.config.ClusterCheck() {
		return &rpc.InitiateJoinResponse{Success: false, Message: "node is already part of a cluster; leave first"}, nil
	}

	targetPort := req.TargetPort
	if targetPort == "" {
		targetPort = "8080"
	}

	remoteClient, err := client.New()
	if err != nil {
		return &rpc.InitiateJoinResponse{Success: false, Message: "failed to create client: " + err.Error()}, nil
	}
	defer remoteClient.Close()
	if err := remoteClient.Connect(req.TargetHost, targetPort, false); err != nil {
		return &rpc.InitiateJoinResponse{Success: false, Message: "failed to connect to target: " + err.Error()}, nil
	}

	hostname, _ := os.Hostname()
	nodeID := req.NodeId
	if nodeID == "" {
		nodeID = s.config.GenerateNodeID()
	}
	bindPort := req.BindPort
	if bindPort == "" {
		bindPort = "8080"
	}

	// Preflight: if a bind IP is provided, verify we can bind to bind_ip:bind_port locally
	if req.BindIp != "" {
		if err := s.preflightBind(req.BindIp, bindPort); err != nil {
			return &rpc.InitiateJoinResponse{Success: false, Message: "bind preflight failed: " + err.Error()}, nil
		}
	}

	joinReq := &rpc.JoinRequest{
		Hostname: hostname,
		Token:    req.Token,
		NodeId:   nodeID,
		BindIp:   req.BindIp,
		BindPort: bindPort,
	}
	jResp, jErr := remoteClient.CLI().Join(context.Background(), joinReq)
	if jErr != nil {
		return &rpc.InitiateJoinResponse{Success: false, Message: "join request failed: " + jErr.Error()}, nil
	}
	if !jResp.Success {
		return &rpc.InitiateJoinResponse{Success: false, Message: jResp.Message}, nil
	}

	// If target returned full cluster config, sync it locally
	if len(jResp.ClusterConfig) > 0 {
		// Ensure our local server knows its own node ID before applying the synced config
		s.config.Pulse.LocalNode = jResp.NodeId
		if _, err := s.ConfigSync(context.Background(), &rpc.ConfigSyncRequest{Config: jResp.ClusterConfig}); err != nil {
			return &rpc.InitiateJoinResponse{Success: false, Message: "config sync failed: " + err.Error()}, nil
		}
	} else {
		// Minimal local update
		s.config.Pulse.LocalNode = jResp.NodeId
		_ = s.config.Save()
		_ = s.Reconfigure()
	}

	return &rpc.InitiateJoinResponse{Success: true, Message: "join initiated"}, nil
}

// preflightBind verifies that we can bind a TCP listener on the given ip:port
// It opens a short-lived listener and closes it immediately.
func (s *Server) preflightBind(ip, port string) error {
	addr := net.JoinHostPort(ip, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	_ = ln.Close()
	return nil
}

// OrchestrateIPFailover moves a set of floating IPs from an old active node to a new active node.
// It brings the IPs down on the old node first (best-effort) and then brings them up on the new node,
// using the server's IP helper RPCs (or local equivalents) grouped per interface according to config.
func (s *Server) OrchestrateIPFailover(oldNodeID, newNodeID string, ips []string) error {
	// Group IPs per interface for old and new nodes based on current configuration
	oldIfaceToIPs, err := s.groupIPsByInterfaceForNode(oldNodeID, ips)
	if err != nil {
		// Old node grouping failure should not block bringing IPs up elsewhere; log and continue
		s.logger.Warn("Failed to map IPs to interfaces on old node", "node", oldNodeID, "error", err)
		oldIfaceToIPs = map[string][]string{}
	}

	newIfaceToIPs, err := s.groupIPsByInterfaceForNode(newNodeID, ips)
	if err != nil {
		return fmt.Errorf("failed to map IPs to interfaces on new node: %w", err)
	}

	// Best-effort: bring down IPs on old node per interface
	if oldNodeID != "" {
		for iface, ipList := range oldIfaceToIPs {
			if oldNodeID == s.config.Pulse.LocalNode {
				// Local: call helper directly
				if _, derr := s.BringDownIP(context.Background(), &rpc.DownIpRequest{Iface: iface, Ips: ipList}); derr != nil {
					s.logger.Warn("Failed to bring IPs down locally on old node", "iface", iface, "error", derr)
				}
			} else {
				if derr := s.bringIPsOnNodeDown(oldNodeID, iface, ipList); derr != nil {
					s.logger.Warn("Failed to bring IPs down on old node", "node", oldNodeID, "iface", iface, "error", derr)
				}
			}
		}
	}

	// Bring up IPs on new node per interface
	for iface, ipList := range newIfaceToIPs {
		if newNodeID == s.config.Pulse.LocalNode {
			// Local: call helper directly
			if _, uerr := s.BringUpIP(context.Background(), &rpc.UpIpRequest{Iface: iface, Ips: ipList}); uerr != nil {
				return fmt.Errorf("failed to bring IPs up locally on iface %s: %w", iface, uerr)
			}
		} else {
			if uerr := s.bringIPsOnNodeUp(newNodeID, iface, ipList); uerr != nil {
				return fmt.Errorf("failed to bring IPs up on node %s iface %s: %w", newNodeID, iface, uerr)
			}
		}
	}

	// For local node, refresh expected IPs for the interfaces involved
	if s.ipMonitor != nil && newNodeID == s.config.Pulse.LocalNode {
		for iface := range newIfaceToIPs {
			// Recompute expected IPs for this interface from authoritative config
			var ifaceIPs []string
			if localNode := s.config.Nodes[newNodeID]; localNode != nil {
				for _, g := range localNode.IPGroups[iface] {
					if grpIPs, ok := s.config.Groups[g]; ok {
						ifaceIPs = append(ifaceIPs, grpIPs...)
					}
				}
			}
			s.ipMonitor.ClearExpectedIPs(iface)
			if len(ifaceIPs) > 0 {
				s.ipMonitor.UpdateExpectedIPs(iface, ifaceIPs)
			}
		}
	}

	return nil
}

// groupIPsByInterfaceForNode maps IPs to interfaces for a specific node based on group assignments
func (s *Server) groupIPsByInterfaceForNode(nodeID string, ips []string) (map[string][]string, error) {
	ifaceToIPs := make(map[string][]string)

	nodeCfg := s.config.Nodes[nodeID]
	if nodeCfg == nil {
		// Try by hostname for backward compatibility
		if nodeID != "" {
			if host, n, err := s.config.GetNodeByHostname(nodeID); err == nil && n != nil {
				_ = host
				nodeCfg = n
			}
		}
	}
	if nodeCfg == nil {
		return nil, fmt.Errorf("node configuration not found for %s", nodeID)
	}

	// Build map group->iface for this node
	groupToIface := make(map[string]string)
	for iface, groups := range nodeCfg.IPGroups {
		for _, g := range groups {
			groupToIface[g] = iface
		}
	}

	// For each IP, find its group in config and interface on this node
	for _, ip := range ips {
		var groupName string
		matched := false
		for g, ipList := range s.config.Groups {
			for _, gip := range ipList {
				if gip == ip {
					groupName = g
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("no group found for IP %s", ip)
		}
		iface, ok := groupToIface[groupName]
		if !ok || iface == "" {
			return nil, fmt.Errorf("group %s not assigned to any interface on node %s", groupName, nodeID)
		}
		ifaceToIPs[iface] = append(ifaceToIPs[iface], ip)
	}
	return ifaceToIPs, nil
}

// bringIPsOnNodeUp contacts a specific node and asks it to bring IPs up on the given interface
func (s *Server) bringIPsOnNodeUp(nodeID, iface string, ips []string) error {
	node := s.config.Nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node configuration not found")
	}
	remoteClient, err := client.New()
	if err != nil {
		return err
	}
	defer remoteClient.Close()
	if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = remoteClient.Server().BringUpIP(ctx, &rpc.UpIpRequest{Iface: iface, Ips: ips})
	return err
}

// bringIPsOnNodeDown contacts a specific node and asks it to bring IPs down on the given interface
func (s *Server) bringIPsOnNodeDown(nodeID, iface string, ips []string) error {
	node := s.config.Nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node configuration not found")
	}
	remoteClient, err := client.New()
	if err != nil {
		return err
	}
	defer remoteClient.Close()
	if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = remoteClient.Server().BringDownIP(ctx, &rpc.DownIpRequest{Iface: iface, Ips: ips})
	return err
}

// GetClusterEpoch returns the current cluster epoch (term)
func (s *Server) GetClusterEpoch() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.clusterEpoch
}

// BroadcastClusterState broadcasts member states and convergence metadata to peers via ConfigSync
func (s *Server) BroadcastClusterState(memberStates map[string]membership.MemberStatus, epoch int64, leaderID string, leases map[string]string) error {
	s.Lock()
	if epoch > s.clusterEpoch {
		s.clusterEpoch = epoch
	}
	s.leaderID = leaderID
	s.Unlock()

	// Build an enhanced JSON payload that includes the current config plus extra fields
	cfgBytes, err := json.Marshal(s.config)
	if err != nil {
		return err
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(cfgBytes, &payload); err != nil {
		return err
	}
	// Attach convergence metadata
	ms := make(map[string]int)
	for id, st := range memberStates {
		ms[id] = int(st)
	}
	payload["member_states"] = ms
	payload["epoch"] = epoch
	payload["leader_id"] = leaderID
	if leases != nil {
		payload["leases"] = leases
	}
	enhancedBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Apply locally via the same path to ensure consistency
	_, _ = s.ConfigSync(context.Background(), &rpc.ConfigSyncRequest{Config: enhancedBytes})

	// Broadcast to peers best-effort
	localID, _ := s.config.GetLocalNodeUUID()
	for peerID, node := range s.config.Nodes {
		if peerID == localID {
			continue
		}
		remoteClient, err := client.New()
		if err != nil {
			continue
		}
		if err := remoteClient.Connect(node.IP, node.Port, false); err != nil {
			remoteClient.Close()
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = remoteClient.Server().ConfigSync(ctx, &rpc.ConfigSyncRequest{Config: enhancedBytes})
		cancel()
		remoteClient.Close()
	}
	return nil
}
