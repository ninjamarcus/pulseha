package membership

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/internal/quorum"
)

// ServerReference is an interface for the server methods needed by the health checker
type ServerReference interface {
	// Add methods that the health checker needs to call on the server
	GetQuorumManager() *quorum.QuorumManager
}

// HealthCheck represents the result of a health check
type HealthCheck struct {
	IP        string
	Available bool
	Latency   time.Duration
	Error     error
}

// HealthChecker handles health checking for nodes and IPs
type HealthChecker struct {
	sync.RWMutex
	members     *MemberList
	checkTicker *time.Ticker
	stopChan    chan struct{}
	stopOnce    sync.Once // Ensure we only close stopChan once
	logger      *logrus.Logger
	ready       bool
	stopped     bool // Track if we're stopped
	server      ServerReference
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(members *MemberList, logger *logrus.Logger) *HealthChecker {
	if logger == nil {
		logger = logrus.New()
	}
	return &HealthChecker{
		members:  members,
		logger:   logger,
		stopChan: make(chan struct{}),
	}
}

// SetServerReference sets the server reference for the health checker
func (h *HealthChecker) SetServerReference(server ServerReference) {
	h.Lock()
	defer h.Unlock()
	h.server = server
	h.logger.Debug("Server reference set for health checker")
}

// Start begins the health checking process
func (h *HealthChecker) Start(interval time.Duration) {
	h.Lock()
	defer h.Unlock()

	if h.stopped {
		h.logger.Debug("Health checker is stopped, reinitializing...")
		h.stopChan = make(chan struct{})
		h.stopped = false
	}
	h.checkTicker = time.NewTicker(interval)
	h.ready = true

	// Add initial delay before starting health checks
	h.logger.Debug("Adding initial delay before starting health checks...")
	time.Sleep(500 * time.Millisecond)

	go h.run()
	h.logger.Debug("Health checker is now running")
}

// Stop halts the health checking process
func (h *HealthChecker) Stop() {
	h.Lock()
	defer h.Unlock()

	h.logger.Debug("Stopping health checker...")

	// Set flags first to prevent new checks from starting
	h.ready = false
	h.stopped = true

	// Stop the ticker
	if h.checkTicker != nil {
		h.checkTicker.Stop()
		h.checkTicker = nil
	}

	// Only close stopChan once
	h.stopOnce.Do(func() {
		h.logger.Debug("Closing stop channel...")
		close(h.stopChan)
	})
}

// run executes the health check loop
func (h *HealthChecker) run() {
	h.logger.Debug("Health check loop started")
	for {
		select {
		case <-h.stopChan:
			h.logger.Debug("Health checker stopping")
			return
		default:
			h.RLock()
			if !h.ready || h.stopped || h.checkTicker == nil {
				h.RUnlock()
				return
			}
			ticker := h.checkTicker
			h.RUnlock()

			select {
			case <-ticker.C:
				h.RLock()
				if !h.ready || h.stopped {
					h.RUnlock()
					return
				}
				h.RUnlock()

				h.logger.Debug("Starting scheduled health check")
				h.performHealthChecks()
			case <-h.stopChan:
				h.logger.Debug("Health checker stopping")
				return
			}
		}
	}
}

// performHealthChecks executes health checks on all nodes and their IPs
func (h *HealthChecker) performHealthChecks() {
	h.RLock()
	defer h.RUnlock()

	memberCount := len(h.members.Members)
	if memberCount == 0 {
		return // No logging needed when no members exist
	}

	// Collect cluster status information for a single consolidated log
	clusterStatus := make([]string, 0, memberCount)
	var failedMembers []string
	var statusChanges []string

	for _, member := range h.members.Members {
		// If this is the local node, just update its health check time
		if member.IsLocal() {
			member.Lock()
			member.LastHCResponse = time.Now()
			member.Latency = "0ms"
			member.Unlock()
			clusterStatus = append(clusterStatus, fmt.Sprintf("%s(local/%s)", 
				member.Hostname, StatusToString(member.Status)))
			continue
		}

		// Store previous state for change detection
		member.Lock()
		wasUnknown := member.Status == StatusUnknown
		member.Unlock()

		// Check node connectivity
		startTime := time.Now()
		isReachable := h.checkNodeConnectivity(member)
		responseTime := time.Since(startTime)

		member.Lock()
		member.LastHCResponse = time.Now()

		if !isReachable {
			// Only set to unknown if it's not already in a known state
			if member.Status != StatusActive && member.Status != StatusPassive {
				member.Status = StatusUnknown
			}
			member.Latency = "N/A"
			member.Unlock()
			
			clusterStatus = append(clusterStatus, fmt.Sprintf("%s(unreachable/%s)", 
				member.Hostname, StatusToString(member.Status)))
			failedMembers = append(failedMembers, member.Hostname)
			continue
		}

		// Node is reachable - update latency once
		member.Latency = fmt.Sprintf("%.2fms", float64(responseTime.Nanoseconds())/1000000)

		// Handle auto-failback for previously failed nodes
		if wasUnknown && h.members.config.Pulse.AutoFailback {
			switch h.members.config.Pulse.Mode {
			case "active-passive":
				activeExists := false
				for _, otherMember := range h.members.Members {
					if otherMember.ID != member.ID && otherMember.Status == StatusActive {
						activeExists = true
						break
					}
				}
				if !activeExists {
					member.Status = StatusActive
					statusChanges = append(statusChanges, fmt.Sprintf("%s promoted to active", member.Hostname))
				} else {
					member.Status = StatusPassive
					statusChanges = append(statusChanges, fmt.Sprintf("%s restored to passive", member.Hostname))
				}
			case "active-active":
				member.Status = StatusPartialActive
				statusChanges = append(statusChanges, fmt.Sprintf("%s restored to partial-active", member.Hostname))
			default:
				member.Status = StatusPassive
				statusChanges = append(statusChanges, fmt.Sprintf("%s restored to passive", member.Hostname))
			}
		} else if member.Status == StatusUnknown {
			member.Status = StatusPassive
			statusChanges = append(statusChanges, fmt.Sprintf("%s recovered to passive", member.Hostname))
		}

		clusterStatus = append(clusterStatus, fmt.Sprintf("%s(%s/%s)", 
			member.Hostname, member.Latency, StatusToString(member.Status)))

		member.Unlock()

		// Check IPs if member has any
		if len(member.ActiveIPs) > 0 {
			failedIPs := h.checkMemberIPs(member)
			if len(failedIPs) > 0 {
				h.logger.Warnf("Member %s has %d failed IPs, initiating redistribution", member.Hostname, len(failedIPs))
				h.handlePartialFailure(member, failedIPs)
			}
		}
	}

	// Log a single consolidated cluster status message
	h.logger.Infof("Cluster health: %s", strings.Join(clusterStatus, ", "))
	
	// Log any status changes
	for _, change := range statusChanges {
		h.logger.Infof("Status change: %s", change)
	}
	
	// Log any failed members
	if len(failedMembers) > 0 {
		h.logger.Warnf("Unreachable members: %s", strings.Join(failedMembers, ", "))
	}
}

// checkNodeConnectivity verifies basic node connectivity
func (h *HealthChecker) checkNodeConnectivity(member *Member) bool {
	// Use member's stored IP and Port directly (no config lookup needed)
	if member.IP == "" || member.Port == "" {
		return false
	}

	// Try to establish basic connection
	address := fmt.Sprintf("%s:%s", member.IP, member.Port)
	conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
	if err == nil {
		conn.Close()
		return true
	}

	return false
}

// checkMemberIPs checks all IPs assigned to a member
func (h *HealthChecker) checkMemberIPs(member *Member) []string {
	var failedIPs []string

	// Create channels for concurrent health checks
	results := make(chan HealthCheck, len(member.ActiveIPs))
	var wg sync.WaitGroup

	// Check each IP concurrently
	for _, ip := range member.ActiveIPs {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			result := h.checkIP(ip)
			results <- result
		}(ip)
	}

	// Wait for all checks to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for result := range results {
		if !result.Available {
			failedIPs = append(failedIPs, result.IP)
		}
	}

	return failedIPs
}

// checkIP performs health check on a single IP
func (h *HealthChecker) checkIP(ip string) HealthCheck {
	start := time.Now()
	h.logger.Debugf("Starting health check for IP: %s", ip)

	// Try to ping the IP with retries
	var lastErr error
	// Reduce retries and timeout for testing
	for i := 0; i < 1; i++ {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:80", ip), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			latency := time.Since(start)
			h.logger.Debugf("Health check successful for IP %s (latency: %v)", ip, latency)
			return HealthCheck{
				IP:        ip,
				Available: true,
				Latency:   latency,
				Error:     nil,
			}
		}
		lastErr = err
		h.logger.Debugf("IP check attempt %d to %s failed: %v", i+1, ip, err)
		time.Sleep(100 * time.Millisecond)
	}

	h.logger.Warnf("Health check failed for IP %s: %v", ip, lastErr)
	return HealthCheck{
		IP:        ip,
		Available: false,
		Latency:   0,
		Error:     lastErr,
	}
}

// handlePartialFailure manages the redistribution of failed IPs
func (h *HealthChecker) handlePartialFailure(member *Member, failedIPs []string) {
	h.logger.Infof("Handling partial failure for member %s with %d failed IPs", member.Hostname, len(failedIPs))

	// Check if quorum is enabled
	quorumEnabled := h.members.config.Pulse.QuorumEnabled

	// Update member status based on mode
	member.Lock()
	switch h.members.config.Pulse.Mode {
	case "active-passive":
		if len(failedIPs) == len(member.ActiveIPs) {
			// All IPs failed in active-passive mode - mark node as down
			h.logger.Warnf("All IPs failed for active node %s, marking as unknown", member.Hostname)

			// If quorum is enabled, we need to initiate a vote before changing status
			if quorumEnabled {
				h.logger.Info("Quorum voting is enabled, initiating vote for node status change")
				member.Unlock() // Unlock before initiating vote

				// Initiate vote through the server component
				voteResult := h.initiateNodeStatusVote(member.ID, StatusUnknown)

				if !voteResult {
					h.logger.Warn("Quorum vote failed, not changing node status")
					return
				}

				h.logger.Info("Quorum vote passed, proceeding with status change")
				member.Lock() // Lock again to continue with status change
			}

			member.Status = StatusUnknown

			// Find a passive node to promote
			for _, otherMember := range h.members.Members {
				if otherMember.ID != member.ID && otherMember.Status == StatusPassive {
					h.logger.Infof("Promoting passive node %s to active", otherMember.Hostname)

					// If quorum is enabled, we need to initiate a vote before promoting
					if quorumEnabled {
						member.Unlock() // Unlock before initiating vote

						// Initiate vote for promotion
						voteResult := h.initiateNodeStatusVote(otherMember.ID, StatusActive)

						if !voteResult {
							h.logger.Warn("Quorum vote failed, not promoting node")
							return
						}

						h.logger.Info("Quorum vote passed, proceeding with promotion")
						member.Lock() // Lock again to continue
					}

					if err := otherMember.MakeActive(member.ActiveIPs); err != nil {
						h.logger.Errorf("Failed to promote passive node: %v", err)
					}
					break
				}
			}
		}

	case "active-active":
		if len(failedIPs) == len(member.ActiveIPs) {
			// All IPs failed in active-active mode - mark as unknown
			h.logger.Warnf("All IPs failed for member %s, marking as unknown", member.Hostname)

			// If quorum is enabled, we need to initiate a vote before changing status
			if quorumEnabled {
				h.logger.Info("Quorum voting is enabled, initiating vote for node status change")
				member.Unlock() // Unlock before initiating vote

				// Initiate vote through the server component
				voteResult := h.initiateNodeStatusVote(member.ID, StatusUnknown)

				if !voteResult {
					h.logger.Warn("Quorum vote failed, not changing node status")
					return
				}

				h.logger.Info("Quorum vote passed, proceeding with status change")
				member.Lock() // Lock again to continue with status change
			}

			member.Status = StatusUnknown
		} else {
			// Partial failure - update status and load factor
			h.logger.Infof("Partial IP failure for member %s, updating status", member.Hostname)

			// If quorum is enabled, we need to initiate a vote before changing to partial active
			if quorumEnabled && member.Status != StatusPartialActive {
				h.logger.Info("Quorum voting is enabled, initiating vote for partial active status")
				member.Unlock() // Unlock before initiating vote

				// Initiate vote through the server component
				voteResult := h.initiateNodeStatusVote(member.ID, StatusPartialActive)

				if !voteResult {
					h.logger.Warn("Quorum vote failed, not changing node status")
					return
				}

				h.logger.Info("Quorum vote passed, proceeding with status change")
				member.Lock() // Lock again to continue with status change
			}

			member.Status = StatusPartialActive
			if member.Capacity > 0 {
				member.LoadFactor = float64(len(member.ActiveIPs)-len(failedIPs)) / float64(member.Capacity)
			}
		}

	default:
		h.logger.Warnf("Unknown cluster mode %s, defaulting to active-passive behavior", h.members.config.Pulse.Mode)
		if len(failedIPs) == len(member.ActiveIPs) {
			// If quorum is enabled, we need to initiate a vote before changing status
			if quorumEnabled {
				h.logger.Info("Quorum voting is enabled, initiating vote for node status change")
				member.Unlock() // Unlock before initiating vote

				// Initiate vote through the server component
				voteResult := h.initiateNodeStatusVote(member.ID, StatusUnknown)

				if !voteResult {
					h.logger.Warn("Quorum vote failed, not changing node status")
					return
				}

				h.logger.Info("Quorum vote passed, proceeding with status change")
				member.Lock() // Lock again to continue with status change
			}

			member.Status = StatusUnknown
		}
	}

	// Remove failed IPs from member
	h.logger.Debugf("Removing failed IPs from member %s: %v", member.Hostname, failedIPs)
	member.RemoveIPs(failedIPs)
	member.Unlock()

	// If quorum is enabled, we need to initiate a vote before redistributing IPs
	if quorumEnabled {
		h.logger.Info("Quorum voting is enabled, initiating vote for IP redistribution")

		// Initiate vote for IP redistribution
		voteResult := h.initiateIPRedistributionVote(failedIPs)

		if !voteResult {
			h.logger.Warn("Quorum vote failed, not redistributing IPs")
			return
		}

		h.logger.Info("Quorum vote passed, proceeding with IP redistribution")
	}

	// Trigger IP redistribution
	h.logger.Info("Initiating IP redistribution for failed IPs")
	if err := h.members.RedistributeIPs(failedIPs); err != nil {
		h.logger.Errorf("Failed to redistribute IPs after partial failure: %v", err)
	} else {
		h.logger.Info("IP redistribution completed successfully")
	}
}

// initiateNodeStatusVote initiates a quorum vote for a node status change
// Returns true if the vote passes or if quorum voting is disabled
func (h *HealthChecker) initiateNodeStatusVote(nodeID string, newStatus MemberStatus) bool {
	h.logger.Infof("Initiating vote for node %s status change to %s", nodeID, statusToString(newStatus))

	// Get the server instance from the context
	if h.server == nil {
		h.logger.Warn("Server reference not available, cannot initiate vote")
		return true // Default to allowing the change if we can't vote
	}

	// Get the quorum manager
	quorumManager := h.server.GetQuorumManager()
	if quorumManager == nil {
		h.logger.Warn("Quorum manager not available, cannot initiate vote")
		return true // Default to allowing the change if quorum manager is not available
	}

	// Check if quorum voting is enabled in the config
	if !h.members.config.Pulse.QuorumEnabled {
		h.logger.Debug("Quorum voting is disabled, allowing node status change without vote")
		return true
	}

	// Get the node hostname for better logging
	var hostname string
	for _, member := range h.members.Members {
		if member.ID == nodeID {
			hostname = member.Hostname
			break
		}
	}

	// Create a descriptive subject and description for the vote
	subject := nodeID
	description := fmt.Sprintf("Change node %s (%s) status to %s", hostname, nodeID, statusToString(newStatus))

	// Initiate the vote through the quorum manager
	sessionID, err := quorumManager.StartVotingSession(
		quorum.VoteTypeNodeStatus,
		subject,
		description,
		30*time.Second, // 30 second timeout for votes
	)

	if err != nil {
		h.logger.Errorf("Failed to start voting session: %v", err)
		return true // Default to allowing the change if we can't start a vote
	}

	h.logger.Infof("Started voting session %s for node status change", sessionID)

	// Get our own node ID to cast our vote
	localNodeID, err := h.members.config.GetLocalNodeUUID()
	if err != nil {
		h.logger.Errorf("Failed to get local node ID: %v", err)
	} else {
		// Cast our own vote (we initiated it, so we vote yes)
		err = quorumManager.CastVote(sessionID, localNodeID, quorum.VoteDecisionYes)
		if err != nil {
			h.logger.Errorf("Failed to cast our own vote: %v", err)
		}
	}

	// Wait for the vote to complete
	// In a production implementation, this would be asynchronous with callbacks
	// For simplicity, we'll use a polling approach here
	for i := 0; i < 30; i++ { // Poll for up to 30 seconds
		time.Sleep(1 * time.Second)

		session, err := quorumManager.GetVotingSession(sessionID)
		if err != nil {
			h.logger.Errorf("Failed to get voting session: %v", err)
			continue
		}

		// Check if the vote has completed
		if session.Result != nil {
			h.logger.Infof("Vote completed: passed=%v, quorum=%v, yes=%d, no=%d, total=%d",
				session.Result.Passed, session.Result.QuorumMet,
				session.Result.YesCount, session.Result.NoCount,
				session.Result.TotalVotes)

			return session.Result.Passed
		}
	}

	h.logger.Warn("Vote timed out, defaulting to allowing the change")
	return true // Default to allowing the change if the vote times out
}

// initiateIPRedistributionVote initiates a quorum vote for IP redistribution
// Returns true if the vote passes or if quorum voting is disabled
func (h *HealthChecker) initiateIPRedistributionVote(ips []string) bool {
	h.logger.Infof("Initiating vote for redistribution of %d IPs", len(ips))

	// Get the server instance from the context
	if h.server == nil {
		h.logger.Warn("Server reference not available, cannot initiate vote")
		return true // Default to allowing the change if we can't vote
	}

	// Get the quorum manager
	quorumManager := h.server.GetQuorumManager()
	if quorumManager == nil {
		h.logger.Warn("Quorum manager not available, cannot initiate vote")
		return true // Default to allowing the change if quorum manager is not available
	}

	// Check if quorum voting is enabled in the config
	if !h.members.config.Pulse.QuorumEnabled {
		h.logger.Debug("Quorum voting is disabled, allowing IP redistribution without vote")
		return true
	}

	// Create a descriptive subject and description for the vote
	ipList := ""
	if len(ips) <= 5 {
		ipList = fmt.Sprintf("%v", ips)
	} else {
		ipList = fmt.Sprintf("%v and %d more", ips[:5], len(ips)-5)
	}

	subject := fmt.Sprintf("redistribute-%d-ips", len(ips))
	description := fmt.Sprintf("Redistribute %d IPs: %s", len(ips), ipList)

	// Initiate the vote through the quorum manager
	sessionID, err := quorumManager.StartVotingSession(
		quorum.VoteTypeIPRedistribution,
		subject,
		description,
		30*time.Second, // 30 second timeout for votes
	)

	if err != nil {
		h.logger.Errorf("Failed to start voting session: %v", err)
		return true // Default to allowing the change if we can't start a vote
	}

	h.logger.Infof("Started voting session %s for IP redistribution", sessionID)

	// Get our own node ID to cast our vote
	localNodeID, err := h.members.config.GetLocalNodeUUID()
	if err != nil {
		h.logger.Errorf("Failed to get local node ID: %v", err)
	} else {
		// Cast our own vote (we initiated it, so we vote yes)
		err = quorumManager.CastVote(sessionID, localNodeID, quorum.VoteDecisionYes)
		if err != nil {
			h.logger.Errorf("Failed to cast our own vote: %v", err)
		}
	}

	// Wait for the vote to complete
	// In a production implementation, this would be asynchronous with callbacks
	// For simplicity, we'll use a polling approach here
	for i := 0; i < 30; i++ { // Poll for up to 30 seconds
		time.Sleep(1 * time.Second)

		session, err := quorumManager.GetVotingSession(sessionID)
		if err != nil {
			h.logger.Errorf("Failed to get voting session: %v", err)
			continue
		}

		// Check if the vote has completed
		if session.Result != nil {
			h.logger.Infof("Vote completed: passed=%v, quorum=%v, yes=%d, no=%d, total=%d",
				session.Result.Passed, session.Result.QuorumMet,
				session.Result.YesCount, session.Result.NoCount,
				session.Result.TotalVotes)

			return session.Result.Passed
		}
	}

	h.logger.Warn("Vote timed out, defaulting to allowing the IP redistribution")
	return true // Default to allowing the change if the vote times out
}

// Helper function to convert MemberStatus to string
func statusToString(status MemberStatus) string {
	return StatusToString(status)
}
