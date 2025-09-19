package membership

import (
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/charmbracelet/log"
	"github.com/syleron/pulseha/internal/quorum"
)

// Object pools for health checker to reduce memory allocations
var (
	memberStatusMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]MemberStatus, 8)
		},
	}
)

// Helper functions for health checker object pools
func getMemberStatusMap() map[string]MemberStatus {
	m := memberStatusMapPool.Get().(map[string]MemberStatus)
	// Clear the map
	for k := range m {
		delete(m, k)
	}
	return m
}

func putMemberStatusMap(m map[string]MemberStatus) {
	if m != nil {
		memberStatusMapPool.Put(m)
	}
}

// ServerReference is an interface for the server methods needed by the health checker
type ServerReference interface {
	// Add methods that the health checker needs to call on the server
	GetQuorumManager() *quorum.QuorumManager
	OrchestrateIPFailover(oldNodeID, newNodeID string, ips []string) error
	// Cluster-state convergence helpers
	GetClusterEpoch() int64
	BroadcastClusterState(memberStates map[string]MemberStatus, epoch int64, leaderID string, leases map[string]string) error
	// Leader getters for lease-based failover
	GetLeaderID() string
	GetLeaderLeaseUntil() time.Time
	// IP monitor refresh
	RefreshLocalMonitorExpectedIPs()
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
	members             *MemberList
	checkTicker         *time.Ticker
	stopChan            chan struct{}
	stopOnce            sync.Once // Ensure we only close stopChan once
	logger              *log.Logger
	ready               bool
	stopped             bool // Track if we're stopped
	server              ServerReference
	lastClusterState    string    // Track last cluster state to only log changes
	checksWithoutChange int       // Counter for periodic status logs
	lastLeaderBroadcast time.Time // suppress elections briefly after leader broadcast
	lastTick            time.Time // last time a check cycle executed
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(members *MemberList, logger *log.Logger) *HealthChecker {
	if logger == nil {
		logger = log.New(nil)
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

// IsRunning returns true if the health checker is currently running
func (h *HealthChecker) IsRunning() bool {
	h.RLock()
	defer h.RUnlock()
	return h.ready && !h.stopped
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
				// Record heartbeat tick
				h.Lock()
				h.lastTick = time.Now()
				h.Unlock()
				h.RLock()
				if !h.ready || h.stopped {
					h.RUnlock()
					return
				}
				h.RUnlock()

				// Removed debug log to reduce noise - health checks run every second
				h.performHealthChecks()
			case <-h.stopChan:
				h.logger.Debug("Health checker stopping")
				return
			}
		}
	}
}

// LastTickTime returns the timestamp of the last check tick
func (h *HealthChecker) LastTickTime() time.Time {
	h.RLock()
	defer h.RUnlock()
	return h.lastTick
}

// performHealthChecks executes health checks on all nodes and their IPs
func (h *HealthChecker) performHealthChecks() {
	h.logger.Info("Starting health check cycle...")

	h.RLock()
	memberCount := len(h.members.Members)
	if memberCount == 0 {
		h.RUnlock()
		h.logger.Warn("No members in cluster, skipping health check")
		return // No logging needed when no members exist
	}
	h.RUnlock()

	// Collect cluster status information for a single consolidated log
	clusterStatus := make([]string, 0, memberCount)
	clusterStatusForComparison := make([]string, 0, memberCount)
	var failedMembers []string
	var statusChanges []string

	// Check if we are a passive node and need to detect active node failure
	h.RLock()
	var localMember *Member
	for _, m := range h.members.Members {
		if m.IsLocal() {
			localMember = m
			break
		}
	}
	membersCopy := h.members.Members
	h.RUnlock()

	for _, member := range membersCopy {
		// If this is the local node, just update its health check time
		if member.IsLocal() {
			member.Lock()
			member.LastHCResponse = time.Now()
			member.Latency = "0ms"
			member.Unlock()
			// Add to display status (local node)
			clusterStatus = append(clusterStatus, fmt.Sprintf("%s(local/%s)",
				member.Hostname, StatusToString(member.Status)))

			// Add to comparison status (without latency for change detection)
			clusterStatusForComparison = append(clusterStatusForComparison, fmt.Sprintf("%s(%s)",
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
			// Mark node as unknown when unreachable
			previousStatus := member.Status
			member.Status = StatusUnknown
			member.Latency = "N/A"
			member.Unlock()

			// Log status change if node went from reachable to unreachable
			if previousStatus != StatusUnknown {
				statusChanges = append(statusChanges, fmt.Sprintf("%s became unreachable (was %s)",
					member.Hostname, StatusToString(previousStatus)))
				// Immediate convergence nudge on status change
				if h.server != nil && previousStatus != StatusUnknown {
					states := getMemberStatusMap()
					for id, m := range membersCopy {
						m.Lock()
						states[id] = m.Status
						m.Unlock()
					}
					_ = h.server.BroadcastClusterState(states, h.server.GetClusterEpoch()+1, h.getCurrentLeaderID(), nil)
					putMemberStatusMap(states)
					h.Lock()
					h.lastLeaderBroadcast = time.Now()
					h.Unlock()
				}
			}

			clusterStatus = append(clusterStatus, fmt.Sprintf("%s(unreachable/%s)",
				member.Hostname, StatusToString(member.Status)))
			clusterStatusForComparison = append(clusterStatusForComparison, fmt.Sprintf("%s(%s)",
				member.Hostname, StatusToString(member.Status)))
			failedMembers = append(failedMembers, member.Hostname)
			continue
		}

		// Node is reachable - update latency once
		member.Latency = fmt.Sprintf("%.2fms", float64(responseTime.Nanoseconds())/1000000)

		// Handle auto-failback for previously failed nodes
		h.RLock()
		autoFailback := h.members.config.Pulse.AutoFailback
		mode := h.members.config.Pulse.Mode
		h.RUnlock()

		if wasUnknown && autoFailback {
			switch mode {
			case "active-passive":
				activeExists := false
				for _, otherMember := range membersCopy {
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

		// Add to display status (with latency for display)
		clusterStatus = append(clusterStatus, fmt.Sprintf("%s(%s/%s)",
			member.Hostname, member.Latency, StatusToString(member.Status)))

		// Add to comparison status (without latency for change detection)
		clusterStatusForComparison = append(clusterStatusForComparison, fmt.Sprintf("%s(%s)",
			member.Hostname, StatusToString(member.Status)))

		member.Unlock()

		// Floating IP health checks are disabled; failover decisions are based solely on node health
	}

	// Sort status for consistent comparison (status without latency)
	sort.Strings(clusterStatusForComparison)
	currentClusterStateForComparison := strings.Join(clusterStatusForComparison, ", ")

	// Sort display status for consistent ordering
	sort.Strings(clusterStatus)
	currentClusterDisplayState := strings.Join(clusterStatus, ", ")

	// Only log if the cluster state has changed (ignoring latency variations)
	if currentClusterStateForComparison != h.lastClusterState {
		h.logger.Infof("Cluster health: %s", currentClusterDisplayState)
		h.lastClusterState = currentClusterStateForComparison
		h.checksWithoutChange = 0

		// Proactively broadcast updated member states so all nodes converge quickly
		if h.server != nil {
			states := getMemberStatusMap()
			for id, m := range membersCopy {
				m.Lock()
				states[id] = m.Status
				m.Unlock()
			}
			_ = h.server.BroadcastClusterState(states, h.server.GetClusterEpoch()+1, h.getCurrentLeaderID(), nil)
			putMemberStatusMap(states)
			h.Lock()
			h.lastLeaderBroadcast = time.Now()
			h.Unlock()
		}
	} else {
		// Increment counter for unchanged state
		h.checksWithoutChange++

		// Heartbeat convergence nudge every 3 checks (~3s) to advance LastResponse and align peers
		if h.server != nil && h.checksWithoutChange%3 == 0 {
			states := getMemberStatusMap()
			for id, m := range membersCopy {
				m.Lock()
				states[id] = m.Status
				// Also advance local LastResponse to now for consistent display
				m.LastHCResponse = time.Now()
				m.Unlock()
			}
			_ = h.server.BroadcastClusterState(states, h.server.GetClusterEpoch()+1, h.getCurrentLeaderID(), nil)
			putMemberStatusMap(states)
		}

		// Log periodic summary every 60 checks (roughly every minute with 1s interval)
		if h.checksWithoutChange >= 60 {
			h.logger.Infof("Cluster stable for 60 checks: %s", currentClusterDisplayState)
			h.checksWithoutChange = 0
		}
	}

	// Log any status changes
	for _, change := range statusChanges {
		h.logger.Infof("Status change: %s", change)
	}

	// Log any failed members (already captured in status change, so skip if unchanged)
	// The cluster state change will already indicate when nodes become unreachable

	// Check for active node failure and initiate failover if needed
	if localMember != nil {
		h.logger.Infof("Local member %s has status %s, checking for need to elect leader", localMember.Hostname, StatusToString(localMember.Status))
		// Always check for active node failure, not just when passive
		h.checkForActiveNodeFailure()
	} else {
		h.logger.Warn("No local member found, cannot check for active node failure")
	}
}

// getCurrentLeaderID returns the ID of the current active node if any
func (h *HealthChecker) getCurrentLeaderID() string {
	h.RLock()
	members := h.members.Members
	h.RUnlock()

	for id, m := range members {
		m.Lock()
		isActive := m.Status == StatusActive
		m.Unlock()
		if isActive {
			return id
		}
	}
	return ""
}

// checkForActiveNodeFailure checks if the active node has failed and initiates failover
func (h *HealthChecker) checkForActiveNodeFailure() {
	h.logger.Info("Checking for active node in cluster...")

	h.RLock()
	members := h.members.Members
	config := h.members.config
	h.RUnlock()

	// Find the active node
	var activeMember *Member
	for _, member := range members {
		if member.Status == StatusActive {
			activeMember = member
			break
		}
	}

	// If no active node exists, we need to elect one immediately
	if activeMember == nil {
		// Trace current member statuses for diagnostics
		var snapshot []string
		for _, m := range members {
			m.Lock()
			snapshot = append(snapshot, fmt.Sprintf("%s:%s", m.Hostname, StatusToString(m.Status)))
			m.Unlock()
		}
		h.logger.Warnf("No active node found in cluster! Members: %s. Initiating leader election immediately.", strings.Join(snapshot, ", "))
		h.electNewActiveNode()
		return
	}

	h.logger.Infof("Active node found: %s", activeMember.Hostname)

	// Check if the active node has been unreachable for too long
	member := activeMember
	member.Lock()
	timeSinceLastResponse := time.Since(member.LastHCResponse)
	isUnreachable := member.Status == StatusUnknown ||
		timeSinceLastResponse > time.Duration(config.Pulse.FailOverLimit)*time.Millisecond
	hostname := member.Hostname
	activeIPs := member.ActiveIPs
	member.Unlock()

	h.logger.Debugf("Active node %s - timeSinceLastResponse: %v, FailOverLimit: %dms, isUnreachable: %v",
		hostname, timeSinceLastResponse, config.Pulse.FailOverLimit, isUnreachable)

	if isUnreachable {
		h.logger.Warnf("Active node %s has been unreachable for %v (limit: %dms), initiating failover",
			hostname, timeSinceLastResponse, config.Pulse.FailOverLimit)

		// Mark the active node as unknown
		member.Lock()
		oldNodeID := member.ID
		activeIPsCopy := append([]string{}, activeIPs...)
		member.Status = StatusUnknown
		member.Unlock()

		// Elect a new active node and transfer IPs
		h.electNewActiveNode()

		// Transfer the failed node's IPs to the new active using server IP helpers
		newActive := h.findActiveNode()
		if newActive != nil && len(activeIPsCopy) > 0 {
			h.logger.Infof("Transferring %d IPs from failed active node to new active", len(activeIPsCopy))
			if h.server != nil {
				if err := h.server.OrchestrateIPFailover(oldNodeID, newActive.ID, activeIPsCopy); err != nil {
					h.logger.Errorf("Failed to transfer IPs to new active node: %v", err)
				} else {
					// Update member IP state
					newActive.Lock()
					newActive.ActiveIPs = append([]string{}, activeIPsCopy...)
					newActive.Unlock()

					member.Lock()
					member.ActiveIPs = nil
					member.Unlock()
				}
			}
		}
	}
}

// electNewActiveNode elects a new active node from available passive nodes
func (h *HealthChecker) electNewActiveNode() {
	h.logger.Info("Starting leader election for new active node")

	// Find the best candidate based on priority:
	// 1. Local node (if passive/unknown)
	// 2. Node with best latency
	// 3. Node that was last known to be active (for faster recovery)

	var bestCandidate *Member
	var bestScore float64 = -1 // Higher score is better

	// Also track the last known active node's timestamp for tie-breaking
	var lastActiveTime time.Time

	for _, member := range h.members.Members {
		member.Lock()
		status := member.Status
		latencyStr := member.Latency
		lastResponse := member.LastHCResponse
		isLocal := member.IsLocal()
		member.Unlock()

		// Skip if already active or truly unreachable
		if status == StatusActive {
			continue
		}

		// Calculate a score for this candidate
		score := float64(0)

		// Local node gets highest priority (score +100)
		if isLocal && (status == StatusPassive || status == StatusUnknown) {
			score += 100
		}

		// Passive nodes get priority over unknown (score +50)
		if status == StatusPassive {
			score += 50
		} else if status == StatusUnknown {
			score += 25 // Still eligible but lower priority
		} else {
			continue // Skip non-eligible nodes
		}

		// Better latency increases score (max +10 for 0ms, decreases with latency)
		if latencyStr != "N/A" && latencyStr != "" {
			if lat, err := time.ParseDuration(strings.TrimSuffix(latencyStr, "ms") + "ms"); err == nil {
				// Score based on latency (10 points for 0ms, decreasing to 0 for 1000ms+)
				latencyScore := math.Max(0, 10-(float64(lat.Milliseconds())/100))
				score += latencyScore
			}
		}

		// Recent response time adds small bonus (for tie-breaking)
		if !lastResponse.IsZero() {
			recency := time.Since(lastResponse)
			if recency < 5*time.Second {
				score += 5
			}
		}

		h.logger.Infof("Election candidate %s: score=%.2f, status=%s, latency=%s, local=%v",
			member.Hostname, score, StatusToString(status), latencyStr, isLocal)

		if score > bestScore {
			bestCandidate = member
			bestScore = score
			lastActiveTime = lastResponse
		} else if score == bestScore && !lastResponse.IsZero() {
			// Tie-breaker: choose node with more recent activity
			if lastResponse.After(lastActiveTime) {
				bestCandidate = member
				lastActiveTime = lastResponse
			}
		}
	}

	if bestCandidate == nil {
		h.logger.Error("No eligible nodes available for promotion to active")
		return
	}

	h.logger.Infof("Selected %s as best candidate for promotion (score: %.2f)",
		bestCandidate.Hostname, bestScore)

	// Determine if we should use voting based on cluster size
	clusterSize := len(h.members.Members)
	h.logger.Debugf("Cluster size: %d nodes", clusterSize)

	if clusterSize >= 3 {
		// 3+ nodes: Use quorum voting for decisions
		h.logger.Info("Cluster has 3+ nodes, using quorum voting for leader election")

		// Check if quorum manager is available
		if h.server != nil && h.server.GetQuorumManager() != nil {
			voteResult := h.initiateNodeStatusVote(bestCandidate.ID, StatusActive)
			if !voteResult {
				h.logger.Warn("Quorum vote failed for promoting node to active, aborting election")
				return
			}
			h.logger.Info("Quorum vote passed, proceeding with promotion")
		} else {
			h.logger.Warn("Quorum manager not available, aborting election to prevent split-brain")
			return
		}
	} else if clusterSize == 2 {
		// 2 nodes: Use time-based tiebreaker to prevent split-brain
		h.logger.Info("2-node cluster detected; preferring local if peer is Unknown/unreachable")

		// In a 2-node cluster, we need to ensure only one node promotes itself
		// Use a deterministic method: the node with the lower ID wins
		localNodeID, err := h.members.config.GetLocalNodeUUID()
		if err != nil {
			h.logger.Errorf("Failed to get local node ID: %v", err)
			return
		}

		// Find the other node
		var otherNodeID string
		var otherStatus MemberStatus = StatusUnknown
		for _, member := range h.members.Members {
			if member.ID != localNodeID {
				otherNodeID = member.ID
				member.Lock()
				otherStatus = member.Status
				member.Unlock()
				break
			}
		}

		// If leader lease expired, immediately promote local
		if h.server != nil {
			leaseUntil := h.server.GetLeaderLeaseUntil()
			h.logger.Debugf("Leader lease until: %v", leaseUntil)
			if !leaseUntil.IsZero() && time.Now().After(leaseUntil) {
				h.logger.Info("Leader lease expired; promoting local")
			} else if leaseUntil.IsZero() {
				h.logger.Info("No leader lease; promoting local")
			}
			if leaseUntil.IsZero() || time.Now().After(leaseUntil) {
				otherStatus = StatusUnknown
			}
		}

		// If the peer is Unknown/unreachable, promote local immediately
		if otherStatus == StatusUnknown {
			h.logger.Info("Peer is Unknown; promoting local immediately")
		} else {
			// Otherwise use deterministic tie-breaker: lower UUID wins
			if localNodeID > otherNodeID {
				h.logger.Infof("This node (%s) has higher ID than other node (%s), deferring promotion",
					localNodeID, otherNodeID)
				// Wait briefly to see if the other node takes over
				time.Sleep(2 * time.Second)
				// Abort if the other node became active
				for _, member := range h.members.Members {
					if member.ID == otherNodeID && member.Status == StatusActive {
						h.logger.Info("Other node has become active, aborting our promotion")
						return
					}
				}
				h.logger.Info("Other node did not become active, proceeding with promotion")
			} else {
				h.logger.Infof("This node (%s) has lower ID than other node (%s), proceeding with promotion",
					localNodeID, otherNodeID)
			}
		}
	}
	// For single node, just promote immediately

	// Promote the best candidate to active
	h.logger.Infof("Promoting node %s to active", bestCandidate.Hostname)

	bestCandidate.Lock()
	bestCandidateID := bestCandidate.ID
	bestCandidate.Status = StatusActive
	bestCandidate.Unlock()

	// Assign floating IPs to the new active node
	if h.server != nil {
		// Get all floating IPs from config
		h.RLock()
		config := h.members.config
		h.RUnlock()

		var allIPs []string
		for _, ips := range config.Groups {
			allIPs = append(allIPs, ips...)
		}

		if len(allIPs) > 0 {
			h.logger.Infof("Assigning %d floating IPs to new active node %s", len(allIPs), bestCandidate.Hostname)
			if err := h.server.OrchestrateIPFailover("", bestCandidateID, allIPs); err != nil {
				h.logger.Errorf("Failed to assign IPs to new active node: %v", err)
			} else {
				// Update member's active IPs tracking
				bestCandidate.Lock()
				bestCandidate.ActiveIPs = append([]string{}, allIPs...)
				bestCandidate.Unlock()

				// Refresh IP monitor expectations if this is the local node
				h.server.RefreshLocalMonitorExpectedIPs()
			}
		}
	}

	h.logger.Infof("Leader election complete, %s is now the active node", bestCandidate.Hostname)

	// Broadcast the new cluster state (increment epoch and set leader) for convergence in active-passive mode
	if h.server != nil {
		states := getMemberStatusMap()
		for id, m := range h.members.Members {
			m.Lock()
			states[id] = m.Status
			m.Unlock()
		}
		_ = h.server.BroadcastClusterState(states, h.server.GetClusterEpoch()+1, bestCandidate.ID, nil)
		putMemberStatusMap(states)
		h.Lock()
		h.lastLeaderBroadcast = time.Now()
		h.Unlock()
	}
}

// findActiveNode returns the current active node
func (h *HealthChecker) findActiveNode() *Member {
	for _, member := range h.members.Members {
		if member.Status == StatusActive {
			return member
		}
	}
	return nil
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

	// Determine if we should use quorum based on cluster size
	clusterSize := len(h.members.Members)
	quorumEnabled := clusterSize >= 3

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

					if h.server != nil {
						if err := h.server.OrchestrateIPFailover(member.ID, otherMember.ID, member.ActiveIPs); err != nil {
							h.logger.Errorf("Failed to promote passive node: %v", err)
						} else {
							h.logger.Infof("Passive node %s promoted to active", otherMember.Hostname)
							// Update member IP state
							otherMember.Lock()
							otherMember.ActiveIPs = append([]string{}, member.ActiveIPs...)
							otherMember.Unlock()

							member.Lock()
							member.ActiveIPs = nil
							member.Unlock()
						}
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
// Returns true if the vote passes or if quorum voting is not applicable
func (h *HealthChecker) initiateNodeStatusVote(nodeID string, newStatus MemberStatus) bool {
	h.logger.Infof("Initiating vote for node %s status change to %s", nodeID, statusToString(newStatus))

	// Check cluster size to determine if voting is needed
	clusterSize := len(h.members.Members)
	if clusterSize < 3 {
		h.logger.Debugf("Cluster has only %d nodes, voting not required (need 3+)", clusterSize)
		return true
	}

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
// Returns true if the vote passes or if quorum voting is not applicable
func (h *HealthChecker) initiateIPRedistributionVote(ips []string) bool {
	h.logger.Infof("Initiating vote for redistribution of %d IPs", len(ips))

	// Check cluster size to determine if voting is needed
	clusterSize := len(h.members.Members)
	if clusterSize < 3 {
		h.logger.Debugf("Cluster has only %d nodes, voting not required for IP redistribution", clusterSize)
		return true
	}

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
