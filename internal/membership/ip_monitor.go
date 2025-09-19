package membership

import (
	"fmt"
	"net"
	"sync"

	log "github.com/charmbracelet/log"
)

// IPMonitor monitors IP addresses on interfaces and ensures they match the expected configuration
type IPMonitor struct {
	sync.RWMutex
	members     *MemberList
	logger      *log.Logger
	expectedIPs map[string][]string // map[interface][]ips
	stopChan    chan struct{}
	stopOnce    sync.Once
	done        chan struct{}
}

// NewIPMonitor creates a new IP monitor
func NewIPMonitor(members *MemberList, logger *log.Logger) *IPMonitor {
	return &IPMonitor{
		members:     members,
		logger:      logger,
		expectedIPs: make(map[string][]string),
		stopChan:    make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// Start begins monitoring IP addresses
func (m *IPMonitor) Start() error {
	m.Lock()
	defer m.Unlock()

	// Initialize the expected IPs from the current member
	if err := m.initializeExpectedIPs(); err != nil {
		return fmt.Errorf("failed to initialize expected IPs: %v", err)
	}

	// Start platform-specific event monitoring (pure event-driven)
	go m.monitorLoop()
	// Start periodic reconcile as a safety net
	go m.periodicReconcile()

	m.logger.Info("IP monitor started")
	return nil
}

// Stop stops the IP monitor
func (m *IPMonitor) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Info("IP monitor stopped")
	})
}

// UpdateExpectedIPs updates the list of expected IPs for an interface
func (m *IPMonitor) UpdateExpectedIPs(iface string, ips []string) {
	m.Lock()
	defer m.Unlock()

	// Create a copy of the IPs slice to avoid external modifications
	ipsCopy := make([]string, len(ips))
	copy(ipsCopy, ips)

	m.expectedIPs[iface] = ipsCopy
	m.logger.Info("Updated expected IPs", "iface", iface, "ips", ips)
}

// RemoveExpectedIPs removes IPs from the expected list for an interface
func (m *IPMonitor) RemoveExpectedIPs(iface string, ips []string) {
	m.Lock()
	defer m.Unlock()

	// Create a map for quick lookup
	toRemove := make(map[string]bool)
	for _, ip := range ips {
		toRemove[ip] = true
	}

	// Filter out the IPs to remove
	current := m.expectedIPs[iface]
	var updated []string
	for _, ip := range current {
		if !toRemove[ip] {
			updated = append(updated, ip)
		}
	}

	m.expectedIPs[iface] = updated
	m.logger.Info("Removed IPs from interface", "iface", iface, "remaining", updated)
}

// ClearExpectedIPs removes all expected IPs for an interface
func (m *IPMonitor) ClearExpectedIPs(iface string) {
	m.Lock()
	defer m.Unlock()

	delete(m.expectedIPs, iface)
	m.logger.Info("Cleared all expected IPs", "iface", iface)
}

// initializeExpectedIPs initializes the expected IPs from the current member
func (m *IPMonitor) initializeExpectedIPs() error {
	// Get the local node ID
	localNodeID, err := m.members.config.GetLocalNodeUUID()
	if err != nil {
		return fmt.Errorf("failed to get local node ID: %v", err)
	}

	// Resolve local member and node config
	localMember := m.members.GetMemberByID(localNodeID)
	if localMember == nil {
		return fmt.Errorf("local member not found")
	}
	nodeCfg, ok := m.members.config.Nodes[localNodeID]
	if !ok || nodeCfg == nil {
		return fmt.Errorf("local node configuration not found")
	}

	// Reset expected IPs first
	m.expectedIPs = make(map[string][]string)

	// Only seed expectations when local role is Active; passives keep expectations empty
	if localMember.Status != StatusActive {
		m.logger.Info("IP monitor initialization: local node not Active; expected IPs left empty")
		return nil
	}

	// Build expected IPs from group assignments in config
	for iface, groups := range nodeCfg.IPGroups {
		var ifaceIPs []string
		for _, g := range groups {
			if ips, ok := m.members.config.Groups[g]; ok {
				ifaceIPs = append(ifaceIPs, ips...)
			}
		}
		if len(ifaceIPs) > 0 {
			ipsCopy := make([]string, len(ifaceIPs))
			copy(ipsCopy, ifaceIPs)
			m.expectedIPs[iface] = ipsCopy
		}
	}

	m.logger.Info("IP monitor initialization complete", "expected_ifaces", len(m.expectedIPs))
	return nil
}

// monitor loop is provided by platform-specific file (e.g., ip_monitor_linux.go)

// getInterfaceIPs gets all IPs assigned to an interface
func (m *IPMonitor) getInterfaceIPs(iface string) ([]string, error) {
	// Get the interface
	intf, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, fmt.Errorf("interface not found: %v", err)
	}

	// Get addresses
	addrs, err := intf.Addrs()
	if err != nil {
		return nil, fmt.Errorf("failed to get addresses: %v", err)
	}

	// Extract IPs
	var ips []string
	for _, addr := range addrs {
		// Parse the address
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ips = append(ips, ipNet.IP.String())
	}

	return ips, nil
}
