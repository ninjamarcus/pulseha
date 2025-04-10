package membership

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/syleron/pulseha/packages/client"
	"github.com/syleron/pulseha/packages/config"
	"github.com/syleron/pulseha/packages/network"
	"github.com/syleron/pulseha/rpc"
)

// MemberStatus represents the current state of a member
type MemberStatus int

const (
	StatusUnknown MemberStatus = iota
	StatusActive
	StatusPassive
	StatusPartialActive // New state for active-active
)

// Member defines our member object
type Member struct {
	sync.Mutex
	ID             string
	Hostname       string
	IP             string // Node's IP address
	Port           string // Node's port
	Status         MemberStatus
	LastHCResponse time.Time
	Latency        string
	Score          int
	Client         *client.Client
	HCBusy         bool
	config         *config.Config
	logger         *logrus.Logger
	memberList     *MemberList

	// Active-Active support
	ActiveIPs     []string // IPs currently hosted by this member
	Capacity      int      // Node capacity for weighted distribution
	PartialActive bool     // Whether node is partially active
	LoadFactor    float64  // Current load factor (0.0-1.0)
}

// NewMember creates a new member instance
func NewMember(id string, hostname string, cfg *config.Config, logger *logrus.Logger) *Member {
	if logger == nil {
		logger = logrus.New()
	}

	member := &Member{
		ID:       id,
		Hostname: hostname,
		Status:   StatusUnknown,
		config:   cfg,
		logger:   logger,
	}

	logger.Debugf("Member instance created successfully for %s (ID: %s)", hostname, id)
	return member
}

// initializeClient initializes the client connection if needed
func (m *Member) initializeClient() error {
	if m.Client != nil {
		return nil
	}

	m.logger.Debugf("Initializing client connection for member %s", m.Hostname)

	// Get node config
	node, ok := m.config.Nodes[m.Hostname]
	if !ok {
		return fmt.Errorf("no configuration found for member %s", m.Hostname)
	}

	// Create new client
	c, err := client.New()
	if err != nil {
		return fmt.Errorf("failed to create client: %v", err)
	}

	// Connect to the member
	if err := c.Connect(node.IP, node.Port, false); err != nil {
		return fmt.Errorf("failed to connect to member %s: %v", m.Hostname, err)
	}

	m.Client = c
	m.logger.Debugf("Client connection initialized for member %s", m.Hostname)
	return nil
}

// MakeActive promotes a member to fully active state (active-passive mode)
func (m *Member) MakeActive(ips []string) error {
	m.Lock()
	defer m.Unlock()

	// Check if we're in active-passive mode
	if m.config.Pulse.Mode != "active-passive" {
		return fmt.Errorf("cannot make fully active in %s mode", m.config.Pulse.Mode)
	}

	// Check if another node is already active
	for _, member := range m.memberList.Members {
		if member.ID != m.ID && member.Status == StatusActive {
			return fmt.Errorf("another node is already active in active-passive mode")
		}
	}

	m.Status = StatusActive
	m.ActiveIPs = ips
	m.PartialActive = false
	m.LoadFactor = 1.0

	return m.BringUpIPs(ips)
}

// MakePartialActive promotes a member to partially active state (active-active mode)
func (m *Member) MakePartialActive(ips []string) error {
	m.Lock()
	defer m.Unlock()

	if m.config.Pulse.Mode == "active-passive" {
		return fmt.Errorf("cannot make partially active in active-passive mode")
	}

	// Calculate load factor based on capacity
	if m.Capacity > 0 {
		m.LoadFactor = float64(len(ips)) / float64(m.Capacity)
	} else {
		m.LoadFactor = float64(len(ips)) / float64(len(m.config.Groups))
	}

	m.Status = StatusPartialActive
	m.ActiveIPs = ips
	m.PartialActive = true

	return m.BringUpIPs(ips)
}

// BringUpIPs brings up the specified IPs on this member
func (m *Member) BringUpIPs(ips []string) error {
	// Get interface from config for these IPs
	iface, err := m.config.GetGroupIface(m.Hostname, "")
	if err != nil {
		return fmt.Errorf("failed to get interface: %v", err)
	}

	// Check if this is the local node
	if m.IsLocal() {
		// Bring up IPs locally
		return m.bringUpIPsLocally(iface, ips)
	} else {
		// Send RPC to remote node
		m.logger.Debugf("Sending request to bring up %d IPs on node %s", len(ips), m.Hostname)
		_, err := m.Client.Send(client.ProtoFunction(client.SendBringUpIP), &rpc.UpIpRequest{
			Iface: iface,
			Ips:   ips,
		})
		return err
	}
}

// bringUpIPsLocally brings up IPs on the local node
func (m *Member) bringUpIPsLocally(iface string, ips []string) error {
	m.logger.Infof("Bringing up %d IPs on interface %s", len(ips), iface)

	// Update the IP monitor if available
	if m.memberList != nil && m.memberList.ipMonitor != nil {
		m.memberList.ipMonitor.UpdateExpectedIPs(iface, ips)
	}

	for _, ip := range ips {
		m.logger.Debugf("Bringing up IP %s on interface %s", ip, iface)

		// Check if IP is already up somewhere else
		exists, existingIface, err := network.CheckIfIPExists(ip)
		if err != nil {
			return fmt.Errorf("failed to check IP existence: %v", err)
		}

		// If IP exists on another interface, bring it down first
		if exists && existingIface != iface {
			m.logger.Warnf("IP %s exists on interface %s, bringing it down first", ip, existingIface)
			if err := network.BringIPdown(existingIface, ip); err != nil {
				m.logger.Errorf("Failed to bring down IP %s from interface %s: %v", ip, existingIface, err)
				// Continue anyway as the IP might have already been removed
			}
		}

		// Bring up the IP on the specified interface
		if err := network.BringIPup(iface, ip); err != nil {
			return fmt.Errorf("failed to bring up IP %s on interface %s: %v", ip, iface, err)
		}

		// Send gratuitous ARP to update network
		if !network.SendGARP(iface, ip) {
			m.logger.Warnf("Failed to send GARP for IP %s on interface %s", ip, iface)
			// Don't return error as the IP is still up
		}

		m.logger.Infof("Successfully brought up IP %s on interface %s", ip, iface)
	}

	return nil
}

// IsLocal checks if this member is the local node
func (m *Member) IsLocal() bool {
	localNodeID, err := m.config.GetLocalNodeUUID()
	if err != nil {
		return false
	}
	return m.ID == localNodeID
}

// RemoveIPs removes the specified IPs from the member's active IPs
func (m *Member) RemoveIPs(ips []string) {
	m.Lock()
	defer m.Unlock()

	// Create a lookup map for IPs to remove
	toRemove := make(map[string]bool)
	for _, ip := range ips {
		toRemove[ip] = true
	}

	// Filter out the IPs to remove
	var newActiveIPs []string
	for _, ip := range m.ActiveIPs {
		if !toRemove[ip] {
			newActiveIPs = append(newActiveIPs, ip)
		}
	}

	// Update active IPs
	m.ActiveIPs = newActiveIPs

	// Update IP monitor if this is the local node and we have a member list
	if m.IsLocal() && m.memberList != nil && m.memberList.ipMonitor != nil {
		// Get interface for these IPs
		iface, err := m.config.GetGroupIface(m.Hostname, "")
		if err != nil {
			m.logger.Errorf("Failed to get interface for IP removal: %v", err)
			return
		}

		// Update the IP monitor
		m.memberList.ipMonitor.RemoveExpectedIPs(iface, ips)
	}
}

// GetHealthStatus returns detailed health information about the member
func (m *Member) GetHealthStatus() MemberHealth {
	m.Lock()
	defer m.Unlock()

	return MemberHealth{
		Hostname:      m.Hostname,
		Status:        m.Status,
		ActiveIPs:     append([]string{}, m.ActiveIPs...),
		LastResponse:  m.LastHCResponse,
		Latency:       m.Latency,
		PartialActive: m.PartialActive,
	}
}

// StatusToString converts a MemberStatus to its string representation
func StatusToString(status MemberStatus) string {
	switch status {
	case StatusActive:
		return "Active"
	case StatusPassive:
		return "Passive"
	case StatusPartialActive:
		return "PartialActive"
	case StatusUnknown:
		return "Unknown"
	default:
		return fmt.Sprintf("Unknown(%d)", status)
	}
}
