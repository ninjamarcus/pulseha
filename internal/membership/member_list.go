package membership

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/packages/config"
)

// MemberList defines our member list object
type MemberList struct {
	sync.RWMutex
	Members   map[string]*Member
	config    *config.Config
	logger    *logrus.Logger
	ipMonitor *IPMonitor
}

// NewMemberList creates a new member list
func NewMemberList(cfg *config.Config, logger *logrus.Logger) *MemberList {
	ml := &MemberList{
		Members: make(map[string]*Member),
		config:  cfg,
		logger:  logger,
	}
	return ml
}

// SetIPMonitor sets the IP monitor reference
func (m *MemberList) SetIPMonitor(monitor *IPMonitor) {
	m.Lock()
	defer m.Unlock()
	m.ipMonitor = monitor
}

// RedistributeIPs handles redistribution of failed IPs to healthy nodes
func (m *MemberList) RedistributeIPs(failedIPs []string) error {
	m.Lock()
	defer m.Unlock()

	// Get available nodes for redistribution
	availableNodes := m.getAvailableNodes()
	if len(availableNodes) == 0 {
		return fmt.Errorf("no available nodes for IP redistribution")
	}

	// Handle distribution based on mode
	switch m.config.Pulse.Mode {
	case "active-passive":
		// In active-passive mode, find the active node or promote one
		activeNode := m.getActiveNode()
		if activeNode == nil {
			// No active node, promote the first available node
			activeNode = availableNodes[0]
			if err := activeNode.MakeActive(failedIPs); err != nil {
				return fmt.Errorf("failed to promote node to active: %v", err)
			}
		} else {
			// Assign all IPs to the active node
			if err := activeNode.BringUpIPs(failedIPs); err != nil {
				return fmt.Errorf("failed to assign IPs to active node: %v", err)
			}
		}

	case "active-active":
		// Calculate IP distribution based on node capacity
		distribution := m.calculateIPDistribution(failedIPs, availableNodes)

		// Apply the new IP assignments
		for _, node := range availableNodes {
			ips := distribution[node.Hostname]
			if len(ips) == 0 {
				continue
			}

			if err := node.MakePartialActive(ips); err != nil {
				m.logger.Errorf("Failed to assign IPs to node %s: %v", node.Hostname, err)
				// Continue with other nodes even if one fails
				continue
			}
		}

	default:
		return fmt.Errorf("invalid cluster mode: %s", m.config.Pulse.Mode)
	}

	return nil
}

// getAvailableNodes returns a list of nodes that can accept new IPs
func (m *MemberList) getAvailableNodes() []*Member {
	var available []*Member
	for _, member := range m.Members {
		// Skip nodes that are down or at capacity
		if member.Status == StatusUnknown {
			continue
		}

		// Check if node has capacity for more IPs
		if m.hasAvailableCapacity(member) {
			available = append(available, member)
		}
	}
	return available
}

// hasAvailableCapacity checks if a node can handle more IPs
func (m *MemberList) hasAvailableCapacity(member *Member) bool {
	// If no capacity is set, assume node can handle more IPs
	if member.Capacity == 0 {
		return true
	}

	// Check if node is under its capacity limit
	return len(member.ActiveIPs) < member.Capacity
}

// calculateIPDistribution determines how to distribute IPs across available nodes
func (m *MemberList) calculateIPDistribution(ips []string, nodes []*Member) map[string][]string {
	distribution := make(map[string][]string)
	if len(nodes) == 0 {
		return distribution
	}

	switch m.config.Pulse.Mode {
	case "active-passive":
		// All IPs go to the active node
		activeNode := m.getActiveNode()
		if activeNode != nil {
			distribution[activeNode.Hostname] = ips
		}

	case "active-active":
		// Calculate total available capacity and current load
		totalCapacity := 0
		nodeCapacities := make(map[string]int)
		currentLoads := make(map[string]float64)

		for _, node := range nodes {
			available := m.getNodeAvailableCapacity(node)
			nodeCapacities[node.Hostname] = available
			totalCapacity += available
			currentLoads[node.Hostname] = node.LoadFactor
		}

		// Distribute IPs based on capacity and current load
		for _, ip := range ips {
			var targetNode *Member
			minLoad := float64(1000000) // High initial value

			// Find node with lowest load
			for _, node := range nodes {
				if nodeCapacities[node.Hostname] <= 0 {
					continue
				}

				if currentLoads[node.Hostname] < minLoad {
					minLoad = currentLoads[node.Hostname]
					targetNode = node
				}
			}

			if targetNode == nil {
				m.logger.Warn("No nodes with available capacity for IP: ", ip)
				continue
			}

			// Assign IP and update load
			distribution[targetNode.Hostname] = append(distribution[targetNode.Hostname], ip)
			nodeCapacities[targetNode.Hostname]--
			currentLoads[targetNode.Hostname] = float64(len(distribution[targetNode.Hostname])) / float64(targetNode.Capacity)
		}
	}

	return distribution
}

// getNodeAvailableCapacity calculates remaining capacity for a node
func (m *MemberList) getNodeAvailableCapacity(member *Member) int {
	if member.Capacity == 0 {
		return 0 // Unlimited capacity
	}
	return member.Capacity - len(member.ActiveIPs)
}

// getActiveNode returns the current active node in the cluster
func (m *MemberList) getActiveNode() *Member {
	for _, member := range m.Members {
		if member.Status == StatusActive {
			return member
		}
	}
	return nil
}

// AddMember adds a new member to the member list
func (m *MemberList) AddMember(nodeID, hostname, bindIP, bindPort string) error {
	m.Lock()
	defer m.Unlock()
	
	m.logger.Debugf("Starting AddMember process for ID: %s", nodeID)

	// Check if member already exists
	if _, exists := m.Members[nodeID]; exists {
		m.logger.Warningf("Member with ID %s already exists in member list", nodeID)
		return nil
	}

	// Create new member instance
	m.logger.Debugf("Creating new member instance for %s (ID: %s)", hostname, nodeID)
	member := &Member{
		ID:       nodeID,
		Hostname: hostname,
		IP:       bindIP,
		Port:     bindPort,
		Status:   StatusPassive,
		config:   m.config,
		logger:   m.logger,
	}

	m.logger.Debugf("Member instance created successfully for %s (ID: %s)", hostname, nodeID)

	// Add member to list
	m.Members[nodeID] = member

	m.logger.Infof("Successfully added member %s (ID: %s) to member list", hostname, nodeID)
	return nil
}

// AddMemberQuiet adds a member with just the ID for backward compatibility
func (m *MemberList) AddMemberQuiet(id string) error {
	// Get node config
	node, ok := m.config.Nodes[id]
	if !ok {
		m.logger.Errorf("No configuration found for member ID %s", id)
		return fmt.Errorf("no configuration found for member ID %s", id)
	}

	return m.AddMember(id, node.Hostname, node.IP, node.Port)
}

// GetMemberByID returns a member by ID
func (m *MemberList) GetMemberByID(id string) *Member {
	return m.Members[id]
}

// GetMemberByHostname returns a member by hostname
func (m *MemberList) GetMemberByHostname(hostname string) *Member {
	for _, member := range m.Members {
		if member.Hostname == hostname {
			return member
		}
	}
	return nil
}

// GetMemberCount returns the total number of members in the list
func (m *MemberList) GetMemberCount() int {
	return len(m.Members)
}

// RemoveMember removes a member from the list
// The id parameter can be either a node ID or a hostname
func (m *MemberList) RemoveMember(id string) error {
	m.Lock()
	defer m.Unlock()

	// First try to find by node ID
	if member, exists := m.Members[id]; exists {
		// Found by node ID
		m.logger.Debugf("Removing member with ID: %s", id)
		// Redistribute IPs if member was active
		if len(member.ActiveIPs) > 0 {
			if err := m.RedistributeIPs(member.ActiveIPs); err != nil {
				m.logger.Errorf("Failed to redistribute IPs for removed member %s: %v", member.Hostname, err)
			}
		}

		// Remove member
		delete(m.Members, id)
		return nil
	}

	// If not found by ID, try to find by hostname
	for _, member := range m.Members {
		if member.Hostname == id {
			// Found by hostname
			m.logger.Debugf("Removing member with hostname: %s (ID: %s)", id, member.ID)
			// Redistribute IPs if member was active
			if len(member.ActiveIPs) > 0 {
				if err := m.RedistributeIPs(member.ActiveIPs); err != nil {
					m.logger.Errorf("Failed to redistribute IPs for removed member %s: %v", member.Hostname, err)
				}
			}

			// Remove member
			delete(m.Members, member.ID)
			return nil
		}
	}

	return fmt.Errorf("member with ID or hostname %s not found", id)
}

// GetMemberByIdentifier returns a member by either node ID or hostname
func (m *MemberList) GetMemberByIdentifier(identifier string) *Member {
	// First try by ID
	if member, exists := m.Members[identifier]; exists {
		return member
	}

	// Then try by hostname
	for _, member := range m.Members {
		if member.Hostname == identifier {
			return member
		}
	}

	return nil
}
